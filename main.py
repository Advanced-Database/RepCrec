import transaction_manager
import sys

if __name__ == '__main__':
    tm = transaction_manager.TransactionManager()
    file_path = sys.argv[1] if len(sys.argv) >= 2 else None
    if file_path:
        print("Getting input from file...")
        try:
            with open(file_path, 'r') as file:
                for line in file:
                    tm.process_line(line)
                    # if not tm.process_line(line):
                    #     print("[Abort!]")
                    #     break
        except IOError:
            print("[ERROR] Cannot open file: {}".format(file_path))
    else:
        print("Getting input from standard input... (type \"exit\" to exit)")
        while True:
            line = input()
            if line.strip() == "exit":
                break
            tm.process_line(line)
            # if not tm.process_line(line):
            #     print("[Abort!]")
            #     break
