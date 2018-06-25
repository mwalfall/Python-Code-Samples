import sys


class CompareVersions:

    """
    Reads in two version numbers and determines which is greater.
    To use: python compare-version-numbers 4.3.2 4.1.2
    (An interview problem.)
    """
    def process(self, input_data):
        """
        This method validates the input data. If valid it has the results printed.
        Otherwise the appropriate error message is printed.
        :param input_data: Raw input data.
        """

        input_version_numbers = [input_data[1], input_data[2]]

        # Process each version number.
        numeric_version_numbers = list()
        for number in input_version_numbers:
            # A version should consist of three parts.
            parts = number.split('.')
            if len(parts) != 3:
                print("ERROR: Invalid version number format: '{}'. Expected: xx.xx.xx format".format(number))
                return False

            # Determine if all parts are numeric.
            is_all_numeric = [part.isdigit() for part in parts]
            if False in is_all_numeric:
                print("ERROR: Invalid version number. All parts of a version number must be numeric and in xx.xx.xx format.")
                return False

            numeric_version_numbers.append([int(part) for part in parts])

        # Submitted values are valid. Print results.
        self.print_results(numeric_version_numbers, input_version_numbers)
        return

    def print_results(self, numeric_values, version_numbers):
        """
        Prints the formatted output.
        :param numeric_values: list contains the numeric values for the version numbers
        :param version_numbers:
        """
        left_values = numeric_values[0]
        right_values = numeric_values[1]

        # Determine if all parts of the version number are equal.
        if left_values[0] == right_values[0] and left_values[1] == right_values[1] and left_values[2] == right_values[2]:
            print('{} = {}'.format(version_numbers[0], version_numbers[1]))
            return

        # Compare major version.
        if left_values[0] != right_values[0]:
            if left_values[0] > right_values[0]:
                print('{} > {}'.format(version_numbers[0], version_numbers[1]))
                return
            else:
                print('{} < {}'.format(version_numbers[0], version_numbers[1]))
                return

        # Compare minor version.
        if left_values[1] != right_values[1]:
            if left_values[1] > right_values[1]:
                print('{} > {}'.format(version_numbers[0], version_numbers[1]))
                return
            else:
                print('{} < {}'.format(version_numbers[0], version_numbers[1]))
                return

        # Compare patch version.
        if left_values[2] > right_values[2]:
            print('{} > {}'.format(version_numbers[0], version_numbers[1]))
            return
        else:
            print('{} < {}'.format(version_numbers[0], version_numbers[1]))
            return

    def is_valid_param_count(self, arguments):
        """
        Ensures that two data arguments are submitted.
        :param arguments: Arguments obtained from the command line.
        :return: True if two data arguments are submitted. Otherwise, False.
        """
        if len(arguments) != 3:
            print("ERROR: Invalid number of arguments. Two version numbers are required in xx.xx.xx format.")
            return False
        return True


def main():
    comparator = CompareVersions()
    if comparator.is_valid_param_count(sys.argv):
        comparator.process(sys.argv)


if __name__ == "__main__":
    main()
