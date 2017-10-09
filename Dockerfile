FROM centos:centos7
ADD smallhands.py /smallhands.py
ADD smallhands/*.py /smallhands/
ADD requirements.txt /requirements.txt
RUN yum install -y epel-release && yum install -y python2-pip python-dateutil && \
  pip install -r requirements.txt
ENTRYPOINT ["/smallhands.py"]
CMD ["--help"]
