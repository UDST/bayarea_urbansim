while ! sudo apt-get update
do
    echo "apt-get update failed, trying again"
    sleep 1
done

while ! sudo apt-get -y install git g++ python-dev unzip
do
    echo "install git g++ or python-dev failed, trying again"
    sleep 1
done

file="/bayarea_urbansim/lib/Anaconda-2.3.0-Linux-x86_64.sh"
if [ -f "$file" ]
    then
    bash /bayarea_urbansim/lib/Anaconda-2.3.0-Linux-x86_64.sh -b -p $HOME/anaconda
else
    wget -P /bayarea_urbansim/lib/ https://3230d63b5fc54e62148e-c95ac804525aac4b6dba79b00b39d1d3.ssl.cf1.rackcdn.com/Anaconda-2.3.0-Linux-x86_64.sh
    bash /bayarea_urbansim/lib/Anaconda-2.3.0-Linux-x86_64.sh -b -p $HOME/anaconda
fi

#put anaconda python in path
export PATH=/home/vagrant/anaconda/bin:$PATH
newpath="$export PATH=/home/vagrant/anaconda/bin:$PATH"
echo $newpath >> /home/vagrant/.bashrc
source ~/.bashrc

wget https://bootstrap.pypa.io/get-pip.py -O - | sudo python

pip install -u pandana
sudo pip install awscli

cd /bayarea_urbansim; pip install -r requirements.txt

