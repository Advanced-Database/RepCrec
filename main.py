import transaction_manager

FILE_PATH = 'testcase/test22'

if __name__ == '__main__':
    tm = transaction_manager.transaction_manager()

    with open(FILE_PATH, 'r') as fh:
        for line in fh:
            li = line.split('//')[0].strip()
            if li:
                tm.get_instructions(li)
