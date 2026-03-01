import os
import shutil

from validate import validate
from validators.referential_validator import validate_referential_integrity
from load import load
from logger import logger
from audit_logger import log_audit
from dq_report import generate_dq_report

# ---------------------------
# FOLDER CONFIG
# ---------------------------
BASE_DIR = "/opt/airflow/"
IN = os.path.join(BASE_DIR, "data/incoming")
PROC = os.path.join(BASE_DIR, "data/processing")
OK = os.path.join(BASE_DIR, "data/processed")
BAD = os.path.join(BASE_DIR, "data/rejected")
FAILED = os.path.join(BASE_DIR, "data/failed")
QUAR = os.path.join(BASE_DIR, "data/quarantine")


def run_etl(filename):

	for path in [PROC, OK, BAD, FAILED, QUAR]:
		os.makedirs(path, exist_ok=True)

	src = os.path.join(IN, filename)
	proc_path = os.path.join(PROC, filename)
	table_name = filename.replace(".csv", "").lower()

	try:
		logger.info(f"START processing file: {filename}")
		shutil.move(src, proc_path)

		# ---------------------------
		# VALIDATION (Schema + Business)
		# ---------------------------
		is_valid, errors, valid_df, quarantine_df = validate(proc_path, filename)

		if not is_valid:
			logger.error(f"FILE REJECTED: {filename} | {errors}")

			log_audit(
				filename=filename,
				table_name=table_name,
				status="REJECTED",
				error_message=str(errors)
			)

			shutil.move(proc_path, os.path.join(BAD, filename))
			return



		# ---------------------------
		# REFERENTIAL VALIDATION (TRANSACTIONS ONLY)
		# ---------------------------
		if filename.lower() == "transactions.csv":
			valid_df, ref_quarantine = validate_referential_integrity(valid_df)

			if not ref_quarantine.empty:
				quarantine_df = (
					ref_quarantine
					if quarantine_df is None or quarantine_df.empty
					else
					quarantine_df.append(ref_quarantine, ignore_index=True)
				)

		# ---------------------------
		# WRITE QUARANTINE ROWS
		# ---------------------------
		if quarantine_df is not None and not quarantine_df.empty:
			quar_file = filename.replace(".csv", "_rows.csv")
			quar_path = os.path.join(QUAR, quar_file)
			quarantine_df.to_csv(quar_path, index=False)

			logger.warning(
				f"{filename} | Quarantined rows written: {len(quarantine_df)}"
			)

		# ---------------------------
		# DQ METRICS
		# ---------------------------
		total_rows = len(valid_df) + (len(quarantine_df) if quarantine_df is not None else 0)
		valid_rows = len(valid_df)
		invalid_rows = total_rows - valid_rows

		dq_score, _ = generate_dq_report(
			filename,
			total_rows,
			valid_rows,
			invalid_rows
		)

		# ---------------------------
		# LOAD ONLY VALID ROWS
		# ---------------------------
		if valid_df is None or valid_df.empty:
			logger.error(f"{filename} | No valid rows to load")

			log_audit(
				filename=filename,
				table_name=table_name,
				status="REJECTED",
				total_rows=total_rows,
				valid_rows=0,
				invalid_rows=invalid_rows,
				dq_score=dq_score,
				error_message="No valid rows after validation"
			)

			shutil.move(proc_path, os.path.join(BAD, filename))
			return



		load(valid_df, filename)

		# ---------------------------
		# AUDIT SUCCESS
		# ---------------------------
		log_audit(
			filename=filename,
			table_name=table_name,
			status="SUCCESS",
			total_rows=total_rows,
			valid_rows=valid_rows,
			invalid_rows=invalid_rows,
			dq_score=dq_score
		)

		shutil.move(proc_path, os.path.join(OK, filename))
		logger.info(f"SUCCESS {filename}")


	except Exception as e:
		logger.exception(f"FAILED processing file: {filename}")

		log_audit(
			filename=filename,
			table_name=table_name,
			status="FAILED",
			error_message=str(e)
		)

		if os.path.exists(proc_path):
			shutil.move(proc_path, os.path.join(FAILED, filename))




	
if __name__ == "__main__":
	run_etl("customers.csv")
