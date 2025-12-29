import os

def parse_folder_custom(folder_path, output_file):
    models = {}

    for root, _, files in os.walk(folder_path):
        for filename in files:
            if filename.endswith('.sql'):
                name = filename[:-4]  # remove .sql

                # suffix after last "__"
                suffix = name.split('__')[-1] if '__' in name else ''

                # relative folder path
                root_rel = os.path.relpath(root, folder_path)

                # Build tags
                tags = set()
                #tags.add('shared')

                if root_rel != '.':
                    tags.add(root_rel)

                tags.add(name)

                if suffix:
                    tags.add(suffix)

                if name not in models:
                    models[name] = tags
                else:
                    models[name].update(tags)

    output_lines = ["version: 2", "", "models:"]

    for name in sorted(models.keys()):
        output_lines.append(f"  - name: {name}")
        output_lines.append(f"    config:")
        output_lines.append(f"      tags:")
        for tag in sorted(models[name]):
            output_lines.append(f"        - {tag}")
        output_lines.append("")

    with open(output_file, 'w') as f:
        f.write("\n".join(output_lines))

    print(f"Formatted output saved to {output_file}")


## Usage
input_path = 'commissions'
output_path = f"_{input_path}.yaml"

parse_folder_custom(input_path, output_path)
