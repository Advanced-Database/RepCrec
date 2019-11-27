import transaction_manager

FILE_PATH = 'testcase/test1'

if __name__ == '__main__':
    print("---------------- Begin to init TM and DM ----------------")
    tm = transaction_manager.TransactionManager()

    with open(FILE_PATH, 'r') as fh:
        print("---------------- Begin to process instructions ----------------")
        for line in fh:
            li = line.split('//')[0].strip()
            if li:
                if li.startswith("==="):
                    break
                else:
                    tm.parse_instruction(li)

    tm.output_site_status()
