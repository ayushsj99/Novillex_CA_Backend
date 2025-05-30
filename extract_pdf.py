import pdfplumber
import pandas as pd
import json
import os
import re


with pdfplumber.open("bank_statement.pdf") as pdf:
    first_page = pdf.pages[0]
    print(first_page.extract_text(
        x_tolerance=3, x_tolerance_ratio=None, y_tolerance=3, layout=False, x_density=7.25, y_density=13, line_dir_render=None, char_dir_render=None
    ))

    im = first_page.to_image(resolution=150)
    words = first_page.extract_words()
    for word in words:
        im.draw_rect((word["x0"], word["top"], word["x1"], word["bottom"]), stroke_width=1)
    im.show()
    
#     custom_settings = {
#     "vertical_strategy": "text",     # use ruling lines
#     "horizontal_strategy": "lines",   # use ruling lines
#     "intersection_tolerance": 1,
#     "snap_tolerance": 0.1,
#     "join_tolerance": 2,
#     "edge_min_length": 3,
#     "min_words_horizontal": 8,
#     "min_words_vertical": 25,
# }
#     table = first_page.extract_tables(table_settings=custom_settings)
#     if table is not None:
#         df = pd.DataFrame(table[1:], columns=table[0])
#         print(df)
#         first_page.to_image().debug_tablefinder(table_settings=custom_settings).show()

#     else:
#         print("No table found on the first page.")