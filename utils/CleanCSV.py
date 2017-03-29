import os
import csv

input_file = r"C:\Users\rhughes\Documents\ZZ_AroraDocuments\Projects\BCAD\ApplicationTesting\Noise Mitigation\weaverprmFLLGISdata.csv"

output_file = r"C:\Users\rhughes\Documents\ZZ_AroraDocuments\Projects\BCAD\ApplicationTesting\Noise Mitigation\weaver_formatted.csv"

csv.field_size_limit(1000)
out = open(output_file, 'wb')
out_writer = csv.writer(out)

with open(input_file, 'rb') as f:
    dialect = csv.Sniffer().sniff(f.read(1024))
    f.seek(0)

    reader = csv.reader(f, dialect)
    for row in reader:
        new_row = []
        for cell in row:
            if len(cell) > 250:
                # print len(cell)
                pass

            cell = cell.strip()
            cell = cell[:50]
            cell = cell.replace(",", " ")
            cell = cell.replace("/", "_")

            new_row.append(cell)

        out_writer.writerow(new_row)

del out_writer
out.close()
