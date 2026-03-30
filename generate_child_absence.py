from datetime import datetime, timedelta
import argparse
import pandas as pd
import os
import sys


if getattr(sys, "frozen", False):
    BASE_DIR = os.path.dirname(sys.executable)
else:
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))

INPUT_BASE = os.path.join(BASE_DIR, "input")
OUTPUT_BASE = os.path.join(BASE_DIR, "output")
PARENT_FILENAME = "parent_absences.xlsx"
OUTPUT_FILE_PREFIX = "PersonAbsenceEntry"


class AbsenceProcessor:

    INPUT_COLUMNS = [
        "METADATA", "PersonAbsenceEntry", "PersonNumber", "AssignmentNumber", "Employer",
        "AbsenceType", "AbsenceReason", "StartDate", "StartTime", "EndDate",
        "StartDateDuration", "EndDateDuration", "AbsenceStatus", "ApprovalStatus",
        "PerAbsenceEntryId"
    ]

    OUTPUT_COLUMNS = [
        "METADATA", "PersonAbsenceEntryDetail", "PersonNumber", "Employer", "AbsenceType", "AbsenceDate",
        "AssignmentNumber", "AbsenceStartDate", "AbsenceStartTime", "Duration", "RowSeq", "PerAbsenceEntryId"
    ]

    def __init__(self, run_date: str):
        self.input_file = os.path.join(INPUT_BASE, run_date, PARENT_FILENAME)
        self.output_dir_dat = os.path.join(OUTPUT_BASE, run_date, "dat")
        self.output_dir_xlsx = os.path.join(OUTPUT_BASE, run_date, "xlsx")

        if not os.path.isfile(self.input_file):
            raise FileNotFoundError(f"Input file not found: {self.input_file}")

        os.makedirs(self.output_dir_dat, exist_ok=True)
        os.makedirs(self.output_dir_xlsx, exist_ok=True)

    @staticmethod
    def parse_date(date_value):
        """Convert Excel / string / datetime to date object."""
        if isinstance(date_value, datetime):
            return date_value.date()
        date_str = str(date_value)[:10]
        return datetime.strptime(date_str, "%Y-%m-%d").date()

    @staticmethod
    def format_time(time_value):
        """Convert Excel / string / datetime to H:MM format."""
        if pd.isna(time_value):
            return ""
        if isinstance(time_value, datetime):
            return f"{time_value.hour}:{time_value.minute:02d}"
        parts = str(time_value).split(":")
        if len(parts) >= 2:
            return f"{int(parts[0])}:{parts[1].zfill(2)}"
        return str(time_value)

    def _generate_child_rows(self, row) -> list[dict]:
        """Generate child rows from a single parent row."""
        start_date = self.parse_date(row["StartDate"])
        end_date = self.parse_date(row["EndDate"])

        if start_date > end_date:
            raise ValueError(
                f"StartDate {start_date} is after EndDate {end_date} "
                f"for PerAbsenceEntryId {row['PerAbsenceEntryId']}"
            )

        child_rows = []
        current_date = start_date
        row_seq = 1

        while current_date <= end_date:
            child_rows.append({
                "METADATA": row["METADATA"],
                "PersonAbsenceEntryDetail": "PersonAbsenceEntryDetail",
                "PersonNumber": row["PersonNumber"],
                "Employer": row["Employer"],
                "AbsenceType": row["AbsenceType"],
                "AbsenceDate": current_date.strftime("%Y/%m/%d"),
                "AssignmentNumber": row["AssignmentNumber"],
                "AbsenceStartDate": current_date.strftime("%Y/%m/%d"),
                "AbsenceStartTime": self.format_time(row["StartTime"]),
                "Duration": self.format_time(row["StartDateDuration"]),
                "RowSeq": row_seq,
                "PerAbsenceEntryId": row["PerAbsenceEntryId"],
            })
            current_date += timedelta(days=1)
            row_seq += 1

        return child_rows

    def _build_output_records(self, row, child_rows: list[dict]) -> list[list]:
        """Build ordered records: parent headers, parent row, child headers, child rows."""
        formatted_parent = {
            **row,
            "StartDate": self.parse_date(row["StartDate"]).strftime("%Y/%m/%d"),
            "EndDate": self.parse_date(row["EndDate"]).strftime("%Y/%m/%d"),
            "StartTime": self.format_time(row["StartTime"]),
        }
        records = [
            self.INPUT_COLUMNS,
            [formatted_parent[col] for col in self.INPUT_COLUMNS],
            self.OUTPUT_COLUMNS,
        ]
        for child_row in child_rows:
            records.append([child_row[col] for col in self.OUTPUT_COLUMNS])
        return records

    def _save_child_file(self, row, assignment_number: str, child_rows: list[dict]):
        """Write child rows as both a pipe-delimited .dat and an .xlsx file."""
        filename = f"{OUTPUT_FILE_PREFIX}_{assignment_number}"
        records = self._build_output_records(row, child_rows)

        def clean(v):
            return "" if str(v) == "nan" else str(v)

        dat_file = os.path.join(self.output_dir_dat, f"{filename}.dat")
        with open(dat_file, "w") as f:
            for record in records:
                f.write("|".join(clean(v) for v in record) + "\n")
        print(f"Saved {dat_file}")

        xlsx_file = os.path.join(self.output_dir_xlsx, f"{filename}.xlsx")
        pd.DataFrame(records).replace("nan", "").to_excel(xlsx_file, index=False, header=False)
        print(f"Saved {xlsx_file}")

    def run(self):
        """Read the parent file and generate child absence files."""
        print(f"Reading: {self.input_file}")
        df_parent = pd.read_excel(self.input_file, dtype=str)

        for idx, row in df_parent.iterrows():
            try:
                child_rows = self._generate_child_rows(row)
                self._save_child_file(row, row["AssignmentNumber"], child_rows)
            except Exception as e:
                print(f"Error processing row {idx}: {e}")

        print(f"Output written to: {self.output_dir_dat} and {self.output_dir_xlsx}")


def _resolve_date_folder() -> str:
    """Return the date folder from CLI arg, or auto-detect from input/."""
    parser = argparse.ArgumentParser(description="Generate child absence files.")
    parser.add_argument(
        "--date", "-d",
        help="Date folder name inside input/ (e.g. 2026-03-30). "
             "Auto-detected if only one folder exists.",
    )
    args = parser.parse_args()

    if args.date:
        return args.date

    if os.environ.get("RUN_DATE"):
        print(f"Using RUN_DATE from environment: {os.environ['RUN_DATE']}")
        return os.environ["RUN_DATE"]

    # Auto-detect: find date folders inside input/
    if not os.path.isdir(INPUT_BASE):
        raise FileNotFoundError(f"Input base directory '{INPUT_BASE}' not found.")

    folders = [
        f for f in os.listdir(INPUT_BASE)
        if os.path.isdir(os.path.join(INPUT_BASE, f))
    ]

    if len(folders) == 1:
        print(f"Auto-detected input folder: {folders[0]}")
        return folders[0]

    if len(folders) == 0:
        raise FileNotFoundError(f"No date folders found inside '{INPUT_BASE}/'.")

    raise ValueError(
        f"Multiple folders found in '{INPUT_BASE}/': {folders}. "
        "Specify one with --date."
    )


if __name__ == "__main__":
    date_folder = _resolve_date_folder()
    processor = AbsenceProcessor(run_date=date_folder)
    processor.run()