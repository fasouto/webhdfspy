from setuptools import setup

def readme():
    with open('README.md') as f:
        return f.read()

setup(name='webhdfspy',
      version='0.2.1',
      description='A wrapper library to access Hadoop HTTP REST API',
      long_description=readme(),
      classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 2.7',
        'Topic :: Software Development :: Libraries',
      ],
      keywords='hadoop webhdfs',
      url='https://github.com/fsouto/webhdfspy',
      author='Fabio Souto',
      author_email='fabio@fabiosouto.me',
      license='MIT',
      install_requires=[
        'requests',
      ],
      include_package_data=True,
      packages=['webhdfspy'],
      zip_safe=False)