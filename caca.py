import os.path
# from webhdfspy import WebHDFSClient
import webhdfspy


TEST_DIR_PATH = '/pywebhdfs'         # path of the testing directory
TEST_DIR = os.path.basename(TEST_DIR_PATH)
TEST_DIR_PARENT = os.path.abspath(os.path.join(TEST_DIR_PATH, os.pardir))


webHDFS = webhdfspy.WebHDFSClient("localhost", 50070, "fabio")

# print(webHDFS.listdir('/'))

# print(webHDFS.mkdir('/foo'))

# print(webHDFS.listdir('/'))

# print(webHDFS.rename('/foo', '/bar'))

# print(webHDFS.listdir('/'))

# webHDFS.mkdir(TEST_DIR_PATH, '777')
webHDFS.create(TEST_DIR_PATH + '/foo.txt', "foobar", True)
print webHDFS.set_replication(TEST_DIR_PATH + '/foo.txt', -2)
caca =  webHDFS.status(TEST_DIR_PATH + '/foo.txt')
print caca
# webHDFS.rename(TEST_DIR_PATH + '/foo.txt', TEST_DIR_PATH + '/bar.txt')
# print dir_content
# dir_filenames = [d['pathSuffix'] for d in dir_content]
# print dir_filenames
# print(webHDFS.status('/ccaa'))
# print(webHDFS.mkdir('/pywebhdfs_test'))
# print(webHDFS.chmod('/pywebhdfs_test', 777))
# print(webHDFS.chmod('/ccaasss', 777))
# print(webHDFS.append('/ccaassstxt', "cacapiiiis"))
# print webHDFS.create('/pywebhdfs_test/foo.txt', "cuenta atras 3", True)
# print webHDFS.open('/pywebhdfs_test/foo.txt')

# webHDFS.mkdir(TEST_DIR_PATH , '777')
# dir_content = webHDFS.listdir('/')
# dir_filenames = (d['pathSuffix'] for d in dir_content)
# print list(dir_filenames)
# print TEST_DIR
# print(webHDFS.listdir('/pywebhdfs_test'))
