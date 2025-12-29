import csv
import re

input_file = "input.csv"
output_file = "output.csv"

# Finds tables in from clauses 
from_clause_pattern = re.compile(
    r'\bfrom\b\s+(.+?)(?=\bwhere\b|\bjoin\b|\bgroup\b|\border\b|;|$)',
    re.IGNORECASE | re.DOTALL
)

# finds table names in join clauses
join_pattern = re.compile(
    r'\bjoin\s+([a-zA-Z0-9_]+(?:\.[a-zA-Z0-9_]+){0,2})',
    re.IGNORECASE
)

# (removes false positives) Strict table name pattern — only letters, digits, underscore, and dots
valid_table_pattern = re.compile(r'^[A-Z0-9_]+(\.[A-Z0-9_]+){0,2}$')

def clean_table_name(name: str) -> str:
    """Clean table name, remove aliases and punctuation, return uppercase."""
    name = name.strip().split()[0]  # remove alias
    name = re.sub(r'[^A-Za-z0-9_.]', '', name)  # strip stray chars
    return name.upper()


results = []

with open(input_file, newline='', encoding='utf-8') as infile:
    reader = csv.DictReader(infile)
    for row in reader:
        script_id = row["script_id"]
        sql_text = row["sql"]

        sql_text = re.sub(r"'[^']*'", '', sql)
        tables = []

        # --- Extract FROM clause tables ---
        from_clauses = from_clause_pattern.findall(sql_text)
        for clause in from_clauses:
            for part in clause.split(","):
                tbl = clean_table_name(part)
                if tbl and valid_table_pattern.match(tbl):
                    tables.append(tbl)

        # --- Extract JOIN clause tables ---
        join_matches = join_pattern.findall(sql_text)
        for match in join_matches:
            tbl = clean_table_name(match)
            if tbl and valid_table_pattern.match(tbl):
                tables.append(tbl)

        # Deduplicate (case-insensitive)
        seen = set()
        captured_tables = []
        for tbl in tables:
            if tbl not in seen:
                seen.add(tbl)
                captured_tables.append(tbl)

        results.append({
            "script_id": script_id,
            "captured_tables": ",".join(captured_tables)
        })


# --- Output results to an exel fie ---
with open(output_file, "w", newline='', encoding='utf-8') as outfile:
    writer = csv.DictWriter(outfile, fieldnames=["script_id", "captured_tables"])
    writer.writeheader()
    writer.writerows(results)

print("✅ Extraction complete. Results saved to", output_file)
