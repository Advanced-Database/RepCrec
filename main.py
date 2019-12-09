import transaction_manager
import sys

if __name__ == '__main__':
    tm = transaction_manager.TransactionManager()

    # Usage:
    # $ python3 main.py [input_file]
    file_path = sys.argv[1] if len(sys.argv) >= 2 else None
    if file_path:
        print("Getting input from {}...".format(file_path))
        try:
            with open(file_path, 'r') as file:
                for line in file:
                    tm.process_line(line)
        except IOError:
            print("[ERROR] Cannot open file: {}".format(file_path))
    else:
        print("Getting input from standard input... (enter \"exit\" to exit)")
        while True:
            line = input()
            if line.strip() == "exit":
                break
            tm.process_line(line)
            print("========================")
