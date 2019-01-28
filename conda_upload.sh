conda install conda-build  
conda install anaconda-client 
PKG_NAME=toa2lai && USER=f0xy  
OS=$TRAVIS_OS_NAME-64  
mkdir ~/conda-bld 
conda config --set anaconda_upload no  
export CONDA_BLD_PATH=~/conda-bld 
export VERSION=$S2_TOA_TO_LAI_VERSION
#conda build --output . -c conda-forge
#export CONDA_PACKAGE=`conda build --output . | grep bz2`
conda build .
#echo $CONDA_PACKAGE
ls -lah $CONDA_BLD_PATH/$OS 
ls -lah $CONDA_BLD_PATH/noarch
ls -lah ~/
anaconda -t $CONDA_UPLOAD_TOKEN upload -u $USER $(ls $CONDA_BLD_PATH/noarch/$PKG_NAME-$VERSION*.tar.bz2) --force

# Only need to change these two variables
#PKG_NAME=siac
#USER=f0xy
#OS=$TRAVIS_OS_NAME-64
#mkdir ~/conda-bld
#conda config --set anaconda_upload no
#export CONDA_BLD_PATH=~/conda-bld
#export VERSION=`date +%Y.%m.%d`
#conda build . -c conda-forge
#ls $CONDA_BLD_PATH/$OS
#anaconda -t $CONDA_UPLOAD_TOKEN upload -u $USER $(ls $CONDA_BLD_PATH/$OS/$PKG_NAME-$VERSION*.tar.bz2) --force
