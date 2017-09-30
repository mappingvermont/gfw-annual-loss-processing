# grab mini conda install package
# source: https://www.atlantic.net/community/howto/install-python-2-7-centos-anaconda/
wget https://repo.continuum.io/miniconda/Miniconda-latest-Linux-x86_64.sh

# install
sudo sh Miniconda-latest-Linux-x86_64.sh -b -p /usr/local/miniconda

# fix path
export PATH=/usr/local/miniconda/bin:$PATH
source ~/.bashrc

# add gdal package
# source: https://github.com/conda-forge/gdal-feedstock
conda config --add channels conda-forge

# install with sudo using full path to conda
sudo /usr/local/miniconda/bin/conda install gdal -y

# fix symbolic link issue
sudo ln -s /usr/lib64/libjpeg.so.62 /usr/lib64/libjpeg.so.8

# test
# ogrinfo --version

# NB
# make sure to use /usr/bin/python from now on-- conda overwrites the path to use it's own python