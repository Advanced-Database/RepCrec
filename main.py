import transaction_manager

FILE_PATH = 'testcase/test1'

if __name__ == '__main__':
    # print("---------------- Begin to init TM and DM ----------------")
    tm = transaction_manager.TransactionManager()

    with open(FILE_PATH, 'r') as fh:
        # print("---------------- Begin to process instructions ----------------")
        for line in fh:
            tm.process_line(line)
