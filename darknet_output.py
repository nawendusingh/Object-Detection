import os
import sys

obj = set()

file_in = open("/home/ubuntu/darknet/result", "r")
file_out = open("/home/ubuntu/darknet/result_label", "w")
for lines in file_in:
    if lines == "\n":
        continue
    if lines.split(":")[-1] == "\n":
        continue
    if lines.split(":")[-1][-2] == "%":

        obj.add(lines.split(":")[0])


for items in obj:
    file_out.write(items)
    file_out.write(",")

if obj.__len__() == 0:
    file_out.write("No item is detected")
file_in.close()
file_out.close()
