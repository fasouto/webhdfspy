#!/usr/bin/env python
#-*- coding: utf-8 -*-
"""
Warning, this tests will destroy everything under /pywebhdfs_test ,
modify TEST_DIR_PATH if you want to change this.
"""
import os.path
import unittest
import webhdfspy
import requests

TEST_DIR_PATH = '/pywebhdfs_test'                              # path of the testing directory
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


class WebHDFSReplicationTests(unittest.TestCase):
    """
    Test the set replication operation
    """
    def setUp(self):
        self.webHDFS = webhdfspy.WebHDFSClient('localhost', 50070, 'fabio')
        self.webHDFS.mkdir(TEST_DIR_PATH)

    def test_replication(self):
        """
        Test if we can change the replication of a file
        """
        self.webHDFS.create(TEST_DIR_PATH + '/foo.txt', "foobar", True)
        self.webHDFS.set_replication(TEST_DIR_PATH + '/foo.txt', 2)
        file_status = self.webHDFS.status(TEST_DIR_PATH + '/foo.txt')
        self.assertEqual(file_status['replication'], 2)

    def test_negative_replication(self):
        """
        Test if we can put a negative replication number
        """
        self.webHDFS.create(TEST_DIR_PATH + '/foo.txt', "foobar", True)
        self.assertRaises(requests.exceptions.HTTPError, self.webHDFS.set_replication, TEST_DIR_PATH + '/foo.txt', -3)

    def tearDown(self):
        self.webHDFS.remove(TEST_DIR_PATH, True)


class WebHDFSChecksumTests(unittest.TestCase):
    """
    Test the GETFILECHECKSUM operation
    """
    def setUp(self):
        self.webHDFS = webhdfspy.WebHDFSClient('localhost', 50070, 'fabio')
        self.webHDFS.mkdir(TEST_DIR_PATH)

    def test_checksum(self):
        """
        Test that the GETFILECHECKSUM operation returns a valid checksum
        """
        self.webHDFS.create(TEST_DIR_PATH + '/foo.txt', "foobar")
        checksum = self.webHDFS.get_checksum(TEST_DIR_PATH + '/foo.txt')
        self.assertEqual(checksum['bytes'], "00000200000000000000000043d7180b6d1dfa6acae636572cd3b70f00000000")
        self.assertEqual(checksum['length'], 28)

    def tearDown(self):
        self.webHDFS.remove(TEST_DIR_PATH, True)


if __name__ == '__main__':
    unittest.main()
