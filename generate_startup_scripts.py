import os

template_path = "scraper-startup-template.sh"
output_folder = "generated_startups"

# Read template
with open(template_path) as f:
    template = f.read()

# Create output directory
os.makedirs(output_folder, exist_ok=True)

# Generate scripts for chunk 1 to 20
for i in range(1, 21):
    script = template.replace("{{CHUNK}}", str(i))
    with open(f"{output_folder}/scraper_startup_{i}.sh", "w") as out_file:
        out_file.write(script)

print("✅ 20 startup scripts generated in 'generated_startups/' folder")
