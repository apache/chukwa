#   Licensed to the Apache Software Foundation (ASF) under one or more
#   contributor license agreements.  See the NOTICE file distributed with
#   this work for additional information regarding copyright ownership.
#   The ASF licenses this file to You under the Apache License, Version 2.0
#   (the "License"); you may not use this file except in compliance with
#   the License.  You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.
# RPM Spec file for Chukwa v.@chukwaVersion@

%define _topdir         @build.dir@
%define _prefix         @rpm.prefix@
%define _conf_dir       @rpm.conf.dir@
%define uid             @rpm.uid@
%define gid             @rpm.gid@
%define name            chukwa
%define summary         Distributed Computing Monitoring Framework.
%define version         @chukwaVersion@
%define release         @chukwaRelease@
%define license         ASF 2.0
%define group           Development/Monitoring
%define source          %{name}-%{version}.tar.gz
%define vendor          Apache Software Fundation
%define packager        Hadoop Chukwa Team
%define buildroot       %{_topdir}/BUILD

Name:      %{name}
Version:   %{version}
Release:   %{release}
Packager:  %{packager}
Vendor:    %{vendor}
License:   %{license}
Summary:   %{summary}
Group:     %{group}
Source0:   %{source}
Prefix:    %{_prefix}
Buildroot: %{buildroot}

%description
Chukwa is the monitoring framework for large scale distributed
clusters.

%prep
%setup -q
%build
mkdir -p %{buildroot}%{_prefix}
if [ -d %{buildroot}%{_prefix} ]; then
    rm -rf %{buildroot}%{_prefix}
fi
mv %{buildroot}/%{name}-%{version} %{buildroot}%{_prefix}
cd %{buildroot}%{_prefix}
mkdir -p %{buildroot}/etc/init.d

cat %{buildroot}%{_prefix}/tools/init.d/chukwa-data-processors | \
sed 's:CHUKWA_USER=chukwa:CHUKWA_USER=%{uid}:' | \
sed 's:CHUKWA_HOME=/usr/local/chukwa:CHUKWA_HOME=%{_prefix}:' | \
sed 's:CHUKWA_CONF_DIR=/usr/local/chukwa/conf:CHUKWA_CONF_DIR=%{_conf_dir}:' > %{buildroot}/etc/init.d/chukwa-data-processors

cat %{buildroot}%{_prefix}/tools/init.d/chukwa-collector | \
sed 's:CHUKWA_USER=chukwa:CHUKWA_USER=%{uid}:' | \
sed 's:CHUKWA_HOME=/usr/local/chukwa:CHUKWA_HOME=%{_prefix}:' | \
sed 's:CHUKWA_CONF_DIR=/usr/local/chukwa/conf:CHUKWA_CONF_DIR=%{_conf_dir}:' > %{buildroot}/etc/init.d/chukwa-collector

cat %{buildroot}%{_prefix}/tools/service/chukwa-agent/run | \
sed 's:CHUKWA_USER=chukwa:CHUKWA_USER=%{uid}:' | \
sed 's:CHUKWA_HOME=/usr/local/chukwa:CHUKWA_HOME=%{_prefix}:' | \
sed 's:CHUKWA_CONF_DIR=/usr/local/chukwa/conf:CHUKWA_CONF_DIR=%{_conf_dir}:' \
> %{buildroot}%{_prefix}/tools/service/chukwa-agent/run.new
mv %{buildroot}%{_prefix}/tools/service/chukwa-agent/run.new %{buildroot}%{_prefix}/tools/service/chukwa-agent/run

cat %{buildroot}%{_prefix}/tools/service/chukwa-df/run | \
sed 's:CHUKWA_USER=chukwa:CHUKWA_USER=%{uid}:' | \
sed 's:CHUKWA_HOME=/usr/local/chukwa:CHUKWA_HOME=%{_prefix}:' | \
sed 's:CHUKWA_CONF_DIR=/usr/local/chukwa/conf:CHUKWA_CONF_DIR=%{_conf_dir}:' \
> %{buildroot}%{_prefix}/tools/service/chukwa-df/run.new
mv %{buildroot}%{_prefix}/tools/service/chukwa-df/run.new %{buildroot}%{_prefix}/tools/service/chukwa-df/run

cat %{buildroot}%{_prefix}/tools/service/chukwa-iostat/run | \
sed 's:CHUKWA_USER=chukwa:CHUKWA_USER=%{uid}:' | \
sed 's:CHUKWA_HOME=/usr/local/chukwa:CHUKWA_HOME=%{_prefix}:' | \
sed 's:CHUKWA_CONF_DIR=/usr/local/chukwa/conf:CHUKWA_CONF_DIR=%{_conf_dir}:' \
> %{buildroot}%{_prefix}/tools/service/chukwa-iostat/run.new
mv %{buildroot}%{_prefix}/tools/service/chukwa-iostat/run.new %{buildroot}%{_prefix}/tools/service/chukwa-iostat/run

cat %{buildroot}%{_prefix}/tools/service/chukwa-netstat/run | \
sed 's:CHUKWA_USER=chukwa:CHUKWA_USER=%{uid}:' | \
sed 's:CHUKWA_HOME=/usr/local/chukwa:CHUKWA_HOME=%{_prefix}:' | \
sed 's:CHUKWA_CONF_DIR=/usr/local/chukwa/conf:CHUKWA_CONF_DIR=%{_conf_dir}:' \
> %{buildroot}%{_prefix}/tools/service/chukwa-netstat/run.new
mv %{buildroot}%{_prefix}/tools/service/chukwa-netstat/run.new %{buildroot}%{_prefix}/tools/service/chukwa-netstat/run

cat %{buildroot}%{_prefix}/tools/service/chukwa-sar/run | \
sed 's:CHUKWA_USER=chukwa:CHUKWA_USER=%{uid}:' | \
sed 's:CHUKWA_HOME=/usr/local/chukwa:CHUKWA_HOME=%{_prefix}:' | \
sed 's:CHUKWA_CONF_DIR=/usr/local/chukwa/conf:CHUKWA_CONF_DIR=%{_conf_dir}:' \
> %{buildroot}%{_prefix}/tools/service/chukwa-sar/run.new
mv %{buildroot}%{_prefix}/tools/service/chukwa-sar/run.new %{buildroot}%{_prefix}/tools/service/chukwa-sar/run

cat %{buildroot}%{_prefix}/tools/service/chukwa-top/run | \
sed 's:CHUKWA_USER=chukwa:CHUKWA_USER=%{uid}:' | \
sed 's:CHUKWA_HOME=/usr/local/chukwa:CHUKWA_HOME=%{_prefix}:' | \
sed 's:CHUKWA_CONF_DIR=/usr/local/chukwa/conf:CHUKWA_CONF_DIR=%{_conf_dir}:' \
> %{buildroot}%{_prefix}/tools/service/chukwa-top/run.new
mv %{buildroot}%{_prefix}/tools/service/chukwa-top/run.new %{buildroot}%{_prefix}/tools/service/chukwa-top/run

cat %{buildroot}%{_prefix}/tools/service/chukwa-ps/run | \
sed 's:CHUKWA_USER=chukwa:CHUKWA_USER=%{uid}:' | \
sed 's:CHUKWA_HOME=/usr/local/chukwa:CHUKWA_HOME=%{_prefix}:' | \
sed 's:CHUKWA_CONF_DIR=/usr/local/chukwa/conf:CHUKWA_CONF_DIR=%{_conf_dir}:' \
> %{buildroot}%{_prefix}/tools/service/chukwa-ps/run.new
mv %{buildroot}%{_prefix}/tools/service/chukwa-ps/run.new %{buildroot}%{_prefix}/tools/service/chukwa-ps/run

cat %{buildroot}%{_prefix}/tools/service/chukwa-collector/run | \
sed 's:CHUKWA_USER=chukwa:CHUKWA_USER=%{uid}:' | \
sed 's:CHUKWA_HOME=/usr/local/chukwa:CHUKWA_HOME=%{_prefix}:' | \
sed 's:CHUKWA_CONF_DIR=/usr/local/chukwa/conf:CHUKWA_CONF_DIR=%{_conf_dir}:' \
> %{buildroot}%{_prefix}/tools/service/chukwa-collector/run.new
mv %{buildroot}%{_prefix}/tools/service/chukwa-collector/run.new %{buildroot}%{_prefix}/tools/service/chukwa-collector/run

cat %{buildroot}%{_prefix}/tools/service/chukwa-pbsnodes/run | \
sed 's:CHUKWA_USER=chukwa:CHUKWA_USER=%{uid}:' | \
sed 's:CHUKWA_HOME=/usr/local/chukwa:CHUKWA_HOME=%{_prefix}:' | \
sed 's:CHUKWA_CONF_DIR=/usr/local/chukwa/conf:CHUKWA_CONF_DIR=%{_conf_dir}:' \
> %{buildroot}%{_prefix}/tools/service/chukwa-pbsnodes/run.new
mv %{buildroot}%{_prefix}/tools/service/chukwa-pbsnodes/run.new %{buildroot}%{_prefix}/tools/service/chukwa-pbsnodes/run

cat %{buildroot}%{_prefix}/tools/service/chukwa-torque/run | \
sed 's:CHUKWA_USER=chukwa:CHUKWA_USER=%{uid}:' | \
sed 's:CHUKWA_HOME=/usr/local/chukwa:CHUKWA_HOME=%{_prefix}:' | \
sed 's:CHUKWA_CONF_DIR=/usr/local/chukwa/conf:CHUKWA_CONF_DIR=%{_conf_dir}:' \
> %{buildroot}%{_prefix}/tools/service/chukwa-torque/run.new
mv %{buildroot}%{_prefix}/tools/service/chukwa-torque/run.new %{buildroot}%{_prefix}/tools/service/chukwa-torque/run

cat %{buildroot}%{_prefix}/tools/service/chukwa-hdfsusage/run | \
sed 's:CHUKWA_USER=chukwa:CHUKWA_USER=%{uid}:' | \
sed 's:CHUKWA_HOME=/usr/local/chukwa:CHUKWA_HOME=%{_prefix}:' | \
sed 's:CHUKWA_CONF_DIR=/usr/local/chukwa/conf:CHUKWA_CONF_DIR=%{_conf_dir}:' \
> %{buildroot}%{_prefix}/tools/service/chukwa-hdfsusage/run.new
mv %{buildroot}%{_prefix}/tools/service/chukwa-hdfsusage/run.new %{buildroot}%{_prefix}/tools/service/chukwa-hdfsusage/run

chmod a+x %{buildroot}%{_prefix}/tools/expire.sh
chmod a+x %{buildroot}/etc/init.d/chukwa-*
chmod -R a+x %{buildroot}%{_prefix}/tools/service/chukwa-*
rm -rf %{buildroot}%{_prefix}/src
rm -rf %{buildroot}%{_prefix}/build.xml
%post
if [ -d /service/chukwa-agent ]; then
  if [ ! -L /service/chukwa-agent ]; then
    rm -rf /service/chukwa-agent
  fi
fi
if [ -d /service/chukwa-df ]; then
  if [ ! -L /service/chukwa-df ]; then
    rm -rf /service/chukwa-df
  fi
fi
if [ -d /service/chukwa-iostat ]; then
  if [ ! -L /service/chukwa-iostat ]; then
    rm -rf /service/chukwa-iostat
  fi
fi
if [ -d /service/chukwa-netstat ]; then
  if [ ! -L /service/chukwa-netstat ]; then
    rm -rf /service/chukwa-netstat
  fi
fi
if [ -d /service/chukwa-ps ]; then
  if [ ! -L /service/chukwa-ps ]; then
    rm -rf /service/chukwa-ps
  fi
fi
if [ -d /service/chukwa-sar ]; then
  if [ ! -L /service/chukwa-sar ]; then
    rm -rf /service/chukwa-sar
  fi
fi
if [ -d /service/chukwa-top ]; then
  if [ ! -L /service/chukwa-top ]; then
    rm -rf /service/chukwa-top
  fi
fi
mkdir -p %{_prefix}
ln -sf %{_prefix}/tools/service/chukwa-agent /service/chukwa-agent
ln -sf %{_prefix}/tools/service/chukwa-df /service/chukwa-df
ln -sf %{_prefix}/tools/service/chukwa-iostat /service/chukwa-iostat
ln -sf %{_prefix}/tools/service/chukwa-netstat /service/chukwa-netstat
ln -sf %{_prefix}/tools/service/chukwa-sar /service/chukwa-sar
ln -sf %{_prefix}/tools/service/chukwa-top /service/chukwa-top
ln -sf %{_prefix}/tools/service/chukwa-ps /service/chukwa-ps
echo "Congratulation!  You have successfully installed Chukwa."
%preun
echo
%postun
rm -f /service/chukwa-*
%files
%defattr(-,%{uid},%{gid})
%{_prefix}
%defattr(-,root,root)
/etc/init.d/chukwa-data-processors
/etc/init.d/chukwa-collector
