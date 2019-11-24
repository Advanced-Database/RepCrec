FILE_PATH = 'testcase/test22'

if __name__ == '__main__':
    with open(FILE_PATH, 'r') as fh:
        for line in fh:
            li = line.split('//')[0].strip()
            if li:
                print(li)
