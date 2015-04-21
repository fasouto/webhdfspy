#!/usr/bin/env python
#-*- coding: utf-8 -*- 

import unittest
import webhdfspy

class WebHDFSTests(unittest.TestCase):
	"""
	Warning, this tests will destroy everything under /pywebhdfs_test
	"""

	def setUp(self):
		self.webHDFS = webhdfspy.WebHDFSClient('localhost', 50070, 'fabio')
		self.test_dir = '/pywebhdfs_test'

	def test_mkdir(self):
		self.webHDFS.mkdir(self.test_dir , '777')
		dir_content = self.webHDFS.listdir('/')
		dir_filenames = (d['pathSuffix'] for d in dir_content)
		self.assertTrue('pywebhdfs_test' in dir_filenames)
	
	def test_chmod(self):
		self.webHDFS.mkdir(self.test_dir , '777')
		self.webHDFS.chmod(self.test_dir, '444')
	 	dir_content = self.webHDFS.listdir('/')
	 	created_dir = [d for d in dir_content if d['pathSuffix'] == 'pywebhdfs_test']
	 	self.assertEqual(created_dir[0]['permission'], '444')

	def tearDown(self):
	 	self.webHDFS.remove(self.test_dir, True)

if __name__ == '__main__':
	unittest.main()