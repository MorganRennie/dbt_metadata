from pathlib import Path
from collections import defaultdict

base_path = Path("[input path]")

if not base_path.exists() or not base_path.is_dir():
    raise FileNotFoundError(f"Base folder not found or not a directory: {base_path}")

folder_outputs = defaultdict(list)

for sql_file in base_path.rglob("*.sql"):
    try:
        model_name = sql_file.stem  # e.g. stg_abs_monr

        # model name removing first clause: 'stg_table_name' -> 'table_name'
        stripped_name = model_name.split('_', 1)[1] if '_' in model_name else model_name

        # source folder under sox_application_monitoring
        try:
            idx = sql_file.parts.index("sox_application_monitoring")
            model_folder = sql_file.parts[idx + 1]
        except (ValueError, IndexError):
            print(f"Skipping file due to path structure: {sql_file}")
            continue

        # tag suffix: 'int' if intermediate folder else 'stg'
        tag_suffix = "int" if "intermediate" in sql_file.parts else "stg"

        folder_to_write = sql_file.parent

        # Build YAML model block with proper indent
        model_block = (
            f"  - name: {model_name}\n"
            f"    config:\n"
            f"      tags:\n"
            f"        - {model_name}\n"
            f"        - {stripped_name}\n"
            f"        - {model_folder}\n"
            f"      schema: [input schema]\n\n"
        )

        folder_outputs[folder_to_write].append(model_block)

    except Exception as e:
        print(f"⚠Error processing file {sql_file}: {e}")
        continue

# Write one file per folder with the new format
for folder, models in folder_outputs.items():
    try:
        # Identify source folder under sox_application_monitoring for filename
        source_folder = folder.parts[folder.parts.index("sox_application_monitoring") + 1]
        tag_suffix = "int" if "intermediate" in folder.parts else "stg"

        output_filename = f"_{source_folder}_{tag_suffix}.txt"
        output_path = folder / output_filename

        output_path.parent.mkdir(parents=True, exist_ok=True)

        # Write header + models
        with open(output_path, 'w') as f:
            f.write("version: 2\n\nmodels:\n")
            f.writelines(models)

        print(f"Created: {output_path}")

    except Exception as e:
        print(f"⚠️ Failed to write file in {folder}: {e}")
