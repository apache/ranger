#!/usr/bin/env python

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from apache_ranger.model.ranger_base import RangerBase
from apache_ranger.utils             import *
from strenum                         import StrEnum

class PrincipalType(StrEnum):
  USER  = 'USER'
  GROUP = 'GROUP'
  ROLE  = 'ROLE'

  @classmethod
  def value_of(cls, val):
    if isinstance(val, PrincipalType):
      return val
    else:
      for key, member in cls.__members__.items():
        if val == member.name or val == member.value:
          return member
      else:
        raise ValueError(f"'{cls.__name__}' enum not found for '{val}'")


class RangerPrincipal(RangerBase):
    def __init__(self, attrs=None):
        if attrs is None:
            attrs = {}

        RangerBase.__init__(self, attrs)

        self.type = attrs.get('type')
        self.name = attrs.get('name')

    def __hash__(self):
        return hash((self.type, self.name))

    def __eq__(self, other):
        return (self.type, self.name) == (other.type, other.name)

    def type_coerce_attrs(self):
        super(RangerPrincipal, self).type_coerce_attrs()

        self.type = PrincipalType.value_of(self.type)
