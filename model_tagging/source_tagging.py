input_file = "input.txt"
output_file = "output.txt"

with open(input_file, 'r') as f:
    lines = f.readlines()

output_lines = []
current_name = None

for line in lines:
    stripped = line.strip()
    output_lines.append(line)

    if stripped.startswith("- name:"):
        # Extract the name value
        current_name = stripped.split(":")[1].strip()
    
    # After description line, add tags
    if stripped.startswith("description:") and current_name:
        indent = ' ' * (len(line) - len(line.lstrip()))
        output_lines.append(f"{indent}tags: ['{current_name}']\n")
        current_name = None  # Reset after use

# Write to output file
with open(output_file, 'w') as f:
    f.writelines(output_lines)

print(f"Updated text written to: {output_file}")
