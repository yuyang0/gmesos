from setuptools import setup, find_packages

version = '0.0.1'

setup(
    name='gmesos',
    version=version,
    description="A pure python implementation of Mesos scheduler and executor using gevent.",
    packages=find_packages(),
    install_requires=['mesos.interface>=0.22.0,<0.22.1.2'],
    platforms=['POSIX'],
    classifiers=[
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
        'Operating System :: POSIX',
        'Programming Language :: Python',
    ],
    author="Yu Yang",
    author_email="yy2012cn@gmail.com",
    url="https://github.com/yuyang0/gmesos",
    download_url = 'https://github.com/yuyang0/gmesos/archive/%s.tar.gz' % version,
)
