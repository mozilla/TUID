
# FOR AMAZON AMI ONLY
# ENSURE THE EC2 INSTANCE IS GIVEN A ROLE THAT ALLOWS IT ACCESS TO S3 AND DISCOVERY
# THIS EXAMPLE WORKS, BUT YOU MAY FIND IT TOO PERMISSIVE
# {
#   "Version": "2012-10-17",
#   "Statement": [
#     {
#       "Effect": "Allow",
#       "NotAction": "iam:*",
#       "Resource": "*"
#     }
#   ]
# }


# NOTE: NODE DISCOVERY WILL ONLY WORK IF PORT 9300 IS OPEN BETWEEN THEM

sudo yum -y update


# SETUP EPHEMERAL DRIVE
yes | sudo mkfs -t ext4 /dev/sdb
sudo mkdir /data1
sudo mount /dev/sdb /data1
sudo sed -i '$ a\\/dev/sdb   /data1       ext4    defaults,nofail  0   2' /etc/fstab
sudo mount -a

#INCREASE FILE LIMITS

sudo sed -i '$ a\fs.file-max = 100000' /etc/sysctl.conf
sudo sed -i '$ a\vm.max_map_count = 262144' /etc/sysctl.conf

sudo sed -i '$ a\root soft nofile 100000' /etc/security/limits.conf
sudo sed -i '$ a\root hard nofile 100000' /etc/security/limits.conf
sudo sed -i '$ a\root soft memlock unlimited' /etc/security/limits.conf
sudo sed -i '$ a\root hard memlock unlimited' /etc/security/limits.conf

sudo sed -i '$ a\ec2-user soft nofile 100000' /etc/security/limits.conf
sudo sed -i '$ a\ec2-user hard nofile 100000' /etc/security/limits.conf
sudo sed -i '$ a\ec2-user soft memlock unlimited' /etc/security/limits.conf
sudo sed -i '$ a\ec2-user hard memlock unlimited' /etc/security/limits.conf

#HAVE CHANGES TAKE EFFECT
sudo sysctl -p
sudo su ec2-user

# PUT A COPY OF THE JRE INTO THIS TEMP DIR
cd /home/ec2-user/
mkdir temp
cd temp

# INSTALL JAVA 8
sudo rpm -i jre-8u131-linux-x64.rpm
sudo alternatives --install /usr/bin/java java /usr/java/default/bin/java 20000
export JAVA_HOME=/usr/java/default

#CHECK IT IS 1.8
java -version

# INSTALL ELASTICSEARCH
cd /home/ec2-user/
wget https://artifacts.elastic.co/downloads/elasticsearch/elasticsearch-6.1.2.tar.gz
tar zxfv elasticsearch-6.1.2.tar.gz
sudo mkdir /usr/local/elasticsearch
sudo cp -R elasticsearch-6.1.2/* /usr/local/elasticsearch/
rm -fr elasticsearch*


# INSTALL CLOUD PLUGIN
cd /usr/local/elasticsearch/
sudo bin/elasticsearch-plugin install -b discovery-ec2

sudo rm -f /usr/local/elasticsearch/config/elasticsearch.yml
sudo rm -f /usr/local/elasticsearch/config/jvm.options
sudo rm -f /usr/local/elasticsearch/config/log4j2.properties


#INSTALL GIT
sudo yum install -y git-core


# INSTALL PIP
cd ~/temp
rm -fr *
curl https://bootstrap.pypa.io/get-pip.py -o get-pip.py
sudo python get-pip.py

#INSTALL SUPERVISOR
sudo yum install -y libffi-devel
sudo yum install -y openssl-devel
sudo yum groupinstall -y "Development tools"

sudo pip install pyopenssl
sudo pip install ndg-httpsclient
sudo pip install pyasn1
sudo pip install requests
sudo pip install supervisor

cd /usr/bin
sudo ln -s /usr/bin/supervisorctl supervisorctl

# GET MEMMON
sudo yum install python-setuptools
sudo easy_install superlance

# SIMPLE PLACE FOR LOGS
sudo chown ec2-user:ec2-user -R /data1
mkdir /data1/logs
cd /
ln -s  /data1/logs /home/ec2-user/logs


# CLONE TUID
cd ~
git clone https://github.com/mozilla/TUID.git

cd ~/TUID/
git checkout master
sudo pip install -r requirements.txt



###############################################################################
# PLACE ALL CONFIG FILES
###############################################################################

# ELASTICSEARCH CONFIG
sudo chown -R ec2-user:ec2-user /usr/local/elasticsearch
cp ~/TUID/resources/config/elasticsearch.yml     /usr/local/elasticsearch/config/elasticsearch.yml
cp ~/TUID/resources/config/es6_jvm.options       /usr/local/elasticsearch/config/jvm.options
cp ~/TUID/resources/config/es6_log4j2.properties /usr/local/elasticsearch/config/log4j2.properties

# SUPERVISOR CONFIG
sudo cp ~/TUID/resources/config/supervisord.conf /etc/supervisord.conf

# START DAEMON (OR THROW ERROR IF RUNNING ALREADY)
sudo /usr/bin/supervisord -c /etc/supervisord.conf

# READ CONFIG
sudo /usr/bin/supervisorctl reread
sudo /usr/bin/supervisorctl update



