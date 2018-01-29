FROM centos:centos7
ADD smallhands.py /smallhands.py
ADD smallhands/*.py /smallhands/
ADD requirements.txt /requirements.txt
RUN yum install -y epel-release && yum install -y python2-pip python-dateutil && yum clean all && rm -rf /var/cache/yum
RUN pip --no-cache-dir install -r requirements.txt
USER nobody
ENTRYPOINT ["/smallhands.py"]
CMD ["--help"]
