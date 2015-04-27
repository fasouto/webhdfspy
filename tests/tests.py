#!/usr/bin/env python
#-*- coding: utf-8 -*-
"""
Warning, this tests will destroy everything under /pywebhdfs_test ,
modify TEST_DIR_PATH if you want to change this.
"""
import os.path
import unittest
import webhdfspy

TEST_DIR_PATH = '/pywebhdfs/testing/whatever'                              # path of the testing directory
TEST_DIR_PARENT = os.path.abspath(os.path.join(TEST_DIR_PATH, os.pardir))  # parent of the testing dir
TEST_DIR = os.path.basename(TEST_DIR_PATH)                                 # name of the testing directory


class WebHDFSDirTests(unittest.TestCase):
    """
    Test the operations to create and remove dirs
    """
    def setUp(self):
        self.webHDFS = webhdfspy.WebHDFSClient('localhost', 50070, 'fabio')

    def test_mkdir(self):
        self.webHDFS.mkdir(TEST_DIR_PATH)
        dir_content = self.webHDFS.listdir(TEST_DIR_PARENT)
        dir_filenames = (d['pathSuffix'] for d in dir_content)
        self.assertIn(TEST_DIR, dir_filenames)

        self.webHDFS.remove(TEST_DIR_PATH)
        dir_content = self.webHDFS.listdir(TEST_DIR_PARENT)
        dir_filenames = (d['pathSuffix'] for d in dir_content)
        self.assertNotIn(TEST_DIR, dir_filenames)

    def tearDown(self):
        self.webHDFS.remove(TEST_DIR_PATH, True)


class WebHDFSWriteTests(unittest.TestCase):
    """
    Test the CREATE and APPEND operations
    """
    def setUp(self):
        self.webHDFS = webhdfspy.WebHDFSClient('localhost', 50070, 'fabio')
        self.webHDFS.mkdir(TEST_DIR_PATH)

    def test_create(self):
        """
        Test that the create operation create a file inside the test directory
        """
        self.webHDFS.create(TEST_DIR_PATH + '/foo.txt', "foobar")
        dir_content = self.webHDFS.listdir(TEST_DIR_PATH)
        dir_filenames = (d['pathSuffix'] for d in dir_content)
        self.assertIn('foo.txt', dir_filenames)

    def test_overwrite(self):
        """
        Test if it can create a file and overwrite it later
        """
        self.webHDFS.create(TEST_DIR_PATH + '/foobar.txt', "foobar")

        self.webHDFS.create(TEST_DIR_PATH + '/foobar.txt', "barfoo", overwrite=True)
        file_data = self.webHDFS.open(TEST_DIR_PATH + '/foobar.txt')
        self.assertEqual(file_data, "barfoo")

    def test_append(self):
        self.webHDFS.create(TEST_DIR_PATH + '/barfoo.txt', "foo", overwrite=True)

        self.webHDFS.append(TEST_DIR_PATH + '/barfoo.txt', "bar")
        file_data = self.webHDFS.open(TEST_DIR_PATH + '/barfoo.txt')
        self.assertEqual(file_data, "foobar")

    def tearDown(self):
        self.webHDFS.remove(TEST_DIR_PATH, True)


class WebHDFSOwnerTests(unittest.TestCase):
    """
    Test the CMMOD and CHOWN operations
    """
    def setUp(self):
        self.webHDFS = webhdfspy.WebHDFSClient('localhost', 50070, 'fabio')

    def test_chmod(self):
        self.webHDFS.mkdir(TEST_DIR_PATH, '777')
        dir_content = self.webHDFS.listdir(TEST_DIR_PARENT)
        created_dir = [d for d in dir_content if d['pathSuffix'] == TEST_DIR]
        self.assertEqual(created_dir[0]['permission'], '777')

        self.webHDFS.chmod(TEST_DIR_PATH, '444')
        dir_content = self.webHDFS.listdir(TEST_DIR_PARENT)
        created_dir = [d for d in dir_content if d['pathSuffix'] == TEST_DIR]
        self.assertEqual(created_dir[0]['permission'], '444')

    def tearDown(self):
        self.webHDFS.remove(TEST_DIR_PATH, True)


class WebHDFSRenameTests(unittest.TestCase):
    """
    Test the RENAME operations
    """
    def setUp(self):
        self.webHDFS = webhdfspy.WebHDFSClient('localhost', 50070, 'fabio')
        self.webHDFS.mkdir(TEST_DIR_PATH)

    def test_rename_file(self):
        """
        Test the rename of a file
        """
        self.webHDFS.create(TEST_DIR_PATH + '/foo.txt', "foobar")
        self.webHDFS.rename(TEST_DIR_PATH + '/foo.txt', TEST_DIR_PATH + '/bar.txt')
        dir_content = self.webHDFS.listdir(TEST_DIR_PATH)
        dir_filenames = (d['pathSuffix'] for d in dir_content)
        self.assertIn('bar.txt', dir_filenames)

    def test_rename_dir(self):
        """
        Test the rename operation in a directory
        """
        self.webHDFS.mkdir(TEST_DIR_PATH + "/foo")
        self.webHDFS.rename(TEST_DIR_PATH + '/foo', TEST_DIR_PATH + '/bar')
        dir_content = self.webHDFS.listdir(TEST_DIR_PATH)
        dir_filenames = (d['pathSuffix'] for d in dir_content)
        self.assertIn('bar', dir_filenames)

    def tearDown(self):
        self.webHDFS.remove(TEST_DIR_PATH, True)


if __name__ == '__main__':
    unittest.main()
