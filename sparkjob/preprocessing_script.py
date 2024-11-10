import json

infile_paths = ["techmap-jobs-export-2020-10_ie.json",
                "techmap-jobs-export-2021-10_ie.json",
                "techmap-jobs-export-2022-10_ie.json"]

outfile_paths = ["preprocessed-jobs-export-2020-10.jsonl",
                 "preprocessed-jobs-export-2020-10.jsonl",
                 "preprocessed-jobs-export-2020-10.jsonl"]

for infile_path, outfile_path in zip(infile_paths,outfile_paths):

    with open(infile_path, 'r', encoding="utf-8") \
        as infile, open(outfile_path, 'w', encoding="utf-8") as outfile:

        for line in infile:
            
            updated_line = line.replace('$', '_')
            updated_line = updated_line.replace('-', '_')
            updated_line = updated_line.replace('__', '_')

            outfile.write(updated_line)


    