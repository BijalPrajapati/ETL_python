import json
import re
import argparse

class Pipeline:
    '''def extract(self, input_file):
        """Extract data from the input file."""
        with open(input_file, 'r') as file:
            data = file.readlines()
        return data'''
    
    def extract(self, input_file):
        """Extract data from the input file."""
        with open(input_file, 'r') as file:
            for line in file:
                yield line # process the file line by line

    def transform(self, data):
        """Transform the extracted data by cleaning and removing duplicates."""
        specific_pattern = r'%[^%]+%'  # Pattern to match and remove %xxx%
        unique_lines = set()  # Set to track unique lines

        processed_count = 0  # Counter for lines processed
        cleaned_count = 0  # Counter for lines cleaned

        transformed_data = []
        for item in data:
            processed_count += 1  # Increment the processed lines counter

            # Remove XML/HTML tags, %xxx% patterns, and double quotes
            cleaned_line = re.sub(r'"', '',  # Remove double quotes
                                  re.sub(specific_pattern, '', 
                                         re.sub(r'<[^>]+>', '', item.strip())))

            # Check if the cleaned line is non-empty, non-whitespace, and does not contain "empty"
            if cleaned_line.strip() and cleaned_line.lower() != "empty" and cleaned_line not in unique_lines:
                unique_lines.add(cleaned_line)  # Add to set to track uniqueness
                transformed_data.append(cleaned_line)  # Add to the final output list
                cleaned_count += 1  # Increment the cleaned lines counter

        # Print the counts
        print(f"Total lines processed: {processed_count}")
        print(f"Total lines cleaned: {cleaned_count}")

        return transformed_data

    def load(self, data, output_file, batch_size=1000):
        """Load the transformed data into the output file."""
        with open(output_file, 'w') as outfile:
            batch = []
            for item in data:
                batch.append(json.dumps({"line": item}) + '\n')
                if len(batch) >= batch_size:
                    outfile.writelines(batch)
                    batch = []
            if batch:  # Write any remaining lines
                outfile.writelines(batch)

    '''def load(self, data, output_file):
        """Load the transformed data into the output file."""
        with open(output_file, 'w') as outfile:
            for item in data:
                json.dump({"line": item}, outfile)
                outfile.write('\n')'''

    def run(self, input_file, output_file):
        """Run the ETL pipeline."""
        extracted = self.extract(input_file)
        transformed = self.transform(extracted)
        self.load(transformed, output_file)

def main():
    # Setting up argument parsing
    parser = argparse.ArgumentParser(description="Run the ETL pipeline on a text file.")
    parser.add_argument('input_file', type=str, help="Path to the input text file.")
    parser.add_argument('output_file', type=str, help="Path to the output JSON Lines file.")

    # Parsing arguments
    args = parser.parse_args()

    # Validate that the input file exists
    try:
        with open(args.input_file, 'r'):
            pass
    except FileNotFoundError:
        print(f"Error: The input file '{args.input_file}' does not exist.")
        exit(1)

    # Run the pipeline
    pipeline = Pipeline()
    pipeline.run(args.input_file, args.output_file)
    print(f"Pipeline completed successfully. Output saved to '{args.output_file}'.")

if __name__ == "__main__":
    main()
