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
%define hdfsusage_uid   @rpm.hdfsusage.uid@
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
export hdfsusage_uid=%{hdfsusage_uid}
if [ -z "${hdfsusage_uid}" ]; then
    export hdfsusage_uid=%{uid}
fi
mkdir -p %{buildroot}%{_prefix}
if [ -d %{buildroot}%{_prefix} ]; then
    rm -rf %{buildroot}%{_prefix}
fi
mv %{buildroot}/%{name}-%{version} %{buildroot}%{_prefix}
cd %{buildroot}%{_prefix}

chmod a+x %{buildroot}%{_prefix}/tools/expire.sh
rm -rf %{buildroot}%{_prefix}/src
rm -rf %{buildroot}%{_prefix}/build.xml
%post
echo "Congratulation!  You have successfully installed Chukwa."
%preun
echo
%postun
%files
%defattr(-,%{uid},%{gid})
%{_prefix}
