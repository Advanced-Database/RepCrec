FILE_PATH = 'testcase/test1'

if __name__ == '__main__':
    with open(FILE_PATH, 'r') as fh:
        for line in fh:
            li = line.lstrip()
            if not (li.startswith("//") or li.startswith("#")):
                print(line.strip())
