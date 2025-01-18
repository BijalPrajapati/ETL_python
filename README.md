#   The pipeline follows these steps:
# 1. Extract: Read all lines of text from an input file into a list.
# 2. Transform:
    Clean each line by:
    Removing specific paFerns.
    Removing empty segments.
    Stripping XML/HTML tags.
    (Extra) Deduplica:ng the cleaned lines.
# 3. Load: Save all the cleaned data as individual JSON objects in a JSON Lines file.
# 4. Extra: Print a message indica:ng the successful comple:on of the pipeline with metrics like the number of lines processed and the number of lines cleaned. Task
# 5. Idenify Inefficiencies:
    The decorators do not add any meaningful func:onality and might be unnecessarily complex. Simplify   or enhance the decorators to contribute to the pipelineʼs efficiency or func:onality.
# 6. Implement Data Cleaning:
    Remove specific patterns:
        "This line is <br/> great" should become "This line is great". 
        "This line is %link_start%great%link_end%." should become "This line is great.".
        Remove empty lines: Skip lines that are empty or contain only whitespace.
        Strip XML/HTML tags: Remove tags like <x/> or <g> from lines such as "This line <x/> is
        <g>great", resul:ng in "This line is great".
        (Extra) Deduplica2on: Ensure that duplicate lines are only saved once in the output.

Validate that the output JSON Lines file (output.jsonl) is correctly formaFed, contains the expected
cleaned data, and handles the pattern mentioned.

#### Expected Output
The output.jsonl should contain the cleaned and processed lines as follows:
{ "line" : "This line is great" }
{ "line" : "This line is great." }
{ "line" : "This line is great" }
{ "line" : "Another line with spaces." }
{ "line" : "This is a duplicate line." }






# ANSWER: 
# python pipeline.py input.txt output.jsonl

## Parallel Processing 
#  For extremely large files, consider splitting the file into chunks and processing them in parallel using libraries like multiprocessing or concurrent.futures. This will require careful handling to merge results while maintaining order if needed.
#   