# -*- coding: utf-8 -*-
import os, sys, time, json, struct, string, pathlib, pprint, cx_Oracle, sqlalchemy, re, copy, ast
import pandas as pd
import pandas
import numpy as np
from collections import OrderedDict, defaultdict, Counter, namedtuple
from decorator import decorator
from concurrent.futures import ProcessPoolExecutor


class Detail(object):
    @property
    def toJson(self):
        return vars(self)


fields = {
    'identityCard': {
        "yikatong": {
            "丰台老人基础信息.csv": {
                'field': "身份证号码",
                'type': str,
                'lower': False,
                'use': True,
                'fileType': 'csv'
            },
            "服务对象.csv": {
                'field': "CARD",
                'type': str,
                'lower': False,
                'use': True,
                'fileType': 'csv'
            }
        },
        "dibao": {
            "FT_MEMBER_INFO": {
                "field": "STATUS_CARDS_NUMBER",
                'type': str,
                'lower': True,
                'use': True,
                'fileType': 'json'
            }
        },
        "shehuifuli": {
            "BA_ORG_MEMBER_INFO": {
                'field': "CARD_ID",
                "type": str,
                "lower": True,
                'use': True,
                'fileType': 'json'
            }
        },
        "困境数据": {
            "困境数据.xlsx": {
                'field': "身份证号",
                "type": str,
                "lower": False,
                'use': True,
                'fileType': 'excel'
            }
        }
    },
    'gender': {
        "yikatong": {
            "丰台老人基础信息.csv": {
                'field': "性别",
                'type': str,
                'use': True,
                'lower': False
            },
            "服务对象.csv": {
                'field': "SEX",
                "type": int,
                "lower": False,
                'use': True,
                'yikatongDict': True
            }
        },
        'dibao': {
            "FT_MEMBER_INFO": {
                'field': "SEX",
                'type': int,
                'lower': True,
                'use': True
            }
        },
        "shehuifuli": {
            "BA_ORG_MEMBER_INFO": {
                'field': "SEX",
                'type': int,
                'lower': True,
                'use': True
            }
        },
        "困境数据": {
            "困境数据.xlsx": {
                'field': "性别",
                "type": str,
                "lower": False,
                'use': True
            }
        }
    },
    'name': {
        "yikatong": {
            "丰台老人基础信息.csv": {
                'field': "姓名",
                'type': str,
                'use': True,
                'lower': False
            },
            "服务对象.csv": {
                'field': "NAME",
                'type': str,
                'lower': False,
                'use': True
            }
        },
        "dibao": {
            "FT_MEMBER_INFO": {
                'field': "NAME",
                'type': str,
                "lower": True,
                'use': True
            }
        },
        "shehuifuli": {
            "BA_ORG_MEMBER_INFO": {
                'field': "NAME",
                'type': str,
                "lower": True,
                'use': True
            }
        },
        "困境数据": {
            "困境数据.xlsx": {
                'field': "姓名",
                "type": str,
                "lower": False,
                'use': True
            }
        }
    },
    'birthDay': {
        "dibao": {
            "FT_MEMBER_INFO": {
                'field': "BIRTH_DATE",
                'type': str,
                'lower': True,
                'use': False
            }
        },
        "shehuifuli": {
            "BA_ORG_MEMBER_INFO": {
                'field': 'BIRTHDAY',
                'type': str,
                'lower': True,
                'use': False
            }
        },
        "yikatong": {
            "服务对象.csv": {
                'field': 'BIRTH',
                'type': int,
                'lower': False,
                'use': False
            }
        }
    },
    'nationality': {
        "yikatong": {
            "丰台老人基础信息.csv": {
                'field': "民族",
                'type': str,
                'use': True,
                'lower': False
            },
            "服务对象.csv": {
                'field': 'FAMILY_NAME',
                'type': str,
                'lower': False,
                'use': True
            }
        },
        "dibao": {
            "FT_MEMBER_INFO": {
                'field': "nationality",
                'type': int,
                'lower': False,
                'use': True
            }
        },
        "shehuifuli": {
            "BA_ORG_MEMBER_INFO": {
                'field': 'nation',
                'type': int,
                'lower': True,
                'use': True
            }
        },
        "困境数据": {
            "困境数据.xlsx": {
                'field': "民族",
                "type": str,
                "lower": False,
                'use': True
            }
        }
    },
    'education': {
        "yikatong": {
            "丰台老人基础信息.csv": {
                'field': "文化程度",
                'type': str,
                'use': True,
                'lower': False
            },
            "服务对象.csv": {
                'field': 'CULTURE',
                'type': str,
                'lower': False,
                'use': True
            }
        },
        "dibao": {
            "FT_MEMBER_INFO": {
                'field': "member_degree_state",
                'type': int,
                'lower': False,
                'use': True
            }
        },
        "shehuifuli": {
            "BA_ORG_MEMBER_INFO": {
                'field': 'education',
                'type': int,
                'lower': True,
                'use': True
            }
        },
        "困境数据": {
            "困境数据.xlsx": {
                'field': "学历",
                "type": str,
                "lower": False,
                'use': True
            }
        }
    },
    'politics': {
        "dibao": {
            "FT_MEMBER_INFO": {
                'field': "political_landscape",
                'type': int,
                'lower': False,
                'use': True
            }
        },
        "shehuifuli": {
            "BA_ORG_MEMBER_INFO": {
                'field': 'political',
                'type': int,
                'lower': True,
                'use': True
            }
        },
        "yikatong": {},
        "困境数据": {
            "困境数据.xlsx": {
                'field': "政治面貌",
                "type": str,
                "lower": False,
                'use': True
            }
        }
    },
    "marriageStatus": {
        "yikatong": {
            "丰台老人基础信息.csv": {
                'field': "婚姻状况",
                'type': str,
                'use': True,
                'lower': False
            }
        },
        "dibao": {
            "FT_MEMBER_INFO": {
                'field': "marriage_state",
                'type': int,
                'lower': False,
                'use': True
            }
        },
        "shehuifuli": {
            "BA_ORG_MEMBER_INFO": {
                'field': 'marital_status',
                'type': int,
                'lower': True,
                'use': True
            }
        },
        "困境数据": {
            "困境数据.xlsx": {
                'field': "婚姻状况",
                "type": str,
                "lower": False,
                'use': True
            }
        }
    },
    "identityType": {
        "dibao": {
            "FT_MEMBER_INFO": {
                'field': "member_society_type",
                'type': str,
                'lower': False,
                'use': True
            }
        },
        "困境数据": {
            "困境数据.xlsx": {
                'field': "身份类型",
                "type": str,
                "lower": False,
                'use': True
            }
        }
    },
    "locate_community": {
        "yikatong": {
            "丰台老人基础信息.csv": {
                'field': "居住社区",
                'type': str,
                'use': True,
                'lower': False
            },
            "服务对象.csv": {
                'field': 'COMMUNITY',
                'type': str,
                'lower': False,
                'use': True
            }
        },
        "dibao": {
            "FT_MEMBER_INFO": {
                'field': "JZH_SQ_SALVAGE_INFO_ID",
                'type': str,
                'lower': True,
                'use': True,
                # 关联另外一个表的字段
                'relateTable': "FT_FAMILY_INFO",
                'relateField': 'COMMUNITY_ID',
                'relateFieldLower': True,
                'relateFileType': "json",
                'relateFieldType': int
            }
        },
        "shehuifuli": {
            "BA_ORG_MEMBER_INFO": {
                'field': 'community_id',
                'type': int,
                'lower': True,
                'use': True,
                'baseDict': False
            }
        },
        "困境数据": {
            "困境数据.xlsx": {
                'field': "社区/居委会",
                "type": str,
                "lower": False,
                'use': True
            }
        }
    },
    "locate_street": {
        "yikatong": {
            "丰台老人基础信息.csv": {
                'field': "居住街道",
                'type': str,
                'use': True,
                'lower': False
            },
            "服务对象.csv": {
                'field': 'STREET',
                'type': str,
                'lower': False,
                'use': True
            }
        },
        "dibao": {
            "FT_MEMBER_INFO": {
                'field': "JZH_SQ_SALVAGE_INFO_ID",
                'type': str,
                'lower': True,
                'use': True,
                # 关联另外一个表的字段
                'relateTable': "FT_FAMILY_INFO",
                'relateField': 'STREET_ID',
                'relateFieldLower': True,
                'relateFileType': "json",
                'relateFieldType': int
            }
        },
        "shehuifuli": {
            "BA_ORG_MEMBER_INFO": {
                'field': 'street_id',
                'type': int,
                'lower': True,
                'use': True,
                'baseDict': False
            }
        },
        "困境数据": {
            "困境数据.xlsx": {
                'field': "街道/乡镇",
                "type": str,
                "lower": False,
                'use': True
            }
        }
    },
    "locate_county": {
        "yikatong": {
            "丰台老人基础信息.csv": {
                'field': "居住区县",
                'type': str,
                'use': True,
                'lower': False
            },
            "服务对象.csv": {
                'field': 'COUNTY',
                'type': str,
                'lower': False,
                'use': True
            }
        },
        "dibao": {
            "FT_MEMBER_INFO": {
                'field': "JZH_SQ_SALVAGE_INFO_ID",
                'type': str,
                'lower': True,
                'use': True,
                # 关联另外一个表的字段
                'relateTable': "FT_FAMILY_INFO",
                'relateField': 'COUNTY_ID',
                'relateFieldLower': True,
                'relateFileType': "json",
                'relateFieldType': int
            }
        },
        "shehuifuli": {
            "BA_ORG_MEMBER_INFO": {
                'field': 'county_id',
                'type': int,
                'lower': True,
                'use': True,
                'baseDict': False
            }
        },
        "困境数据": {
            "困境数据.xlsx": {
                'field': "区",
                "type": str,
                "lower": False,
                'use': True
            }
        }
    },
    "address": {
        "yikatong": {
            "丰台老人基础信息.csv": {
                'field': "RESIDENCE_ADDRESS",
                'type': str,
                'use': True,
                'lower': False
            },
            "服务对象.csv": {
                'field': 'ADDRESS',
                'type': str,
                'lower': False,
                'use': True
            }
        },
        "dibao": {
            "FT_MEMBER_INFO": {
                'field': "address",
                'type': str,
                'lower': False,
                'use': True
            }
        },
        "shehuifuli": {
            "BA_ORG_MEMBER_INFO": {
                'field': 'address',
                'type': str,
                'lower': False,
                'use': True
            }
        },
        "困境数据": {
            "困境数据.xlsx": {
                'field': "地址",
                "type": str,
                "lower": False,
                'use': True
            }
        }
    },
    'postcode': {
        "yikatong": {
            "丰台老人基础信息.csv": {
                'field': "邮编",
                'type': str,
                'use': True,
                'lower': False
            }
        },
        "dibao": {
            "FT_MEMBER_INFO": {
                'field': "zip",
                'type': str,
                'lower': False,
                'use': True
            }
        },
        "shehuifuli": {
            "BA_ORG_MEMBER_INFO": {
                'field': 'zip_code',
                'type': str,
                'lower': False,
                'use': True
            }
        },
        "困境数据": {
            "困境数据.xlsx": {
                'field': "邮政编码",
                "type": str,
                "lower": False,
                'use': True
            }
        }
    },
    "censusRegister": {
        "yikatong": {
            "丰台老人基础信息.csv": {
                'field': "户口所在地址",
                'type': str,
                'use': True,
                'lower': False
            },
            "服务对象.csv": {
                'field': 'DOMICILE',
                'type': str,
                'lower': False,
                'use': True
            }
        },
        "dibao": {
            "FT_MEMBER_INFO": {
                'field': "household_registery_address",
                'type': str,
                'lower': False,
                'use': True
            }
        },
        "shehuifuli": {
            "BA_ORG_MEMBER_INFO": {
                'field': 'residence_address',
                'type': str,
                'lower': False,
                'use': True
            }
        },
        "困境数据": {
            "困境数据.xlsx": {
                'field': "户籍",
                "type": str,
                "lower": False,
                'use': True
            }
        }
    },

    'registerNature': {
        "dibao": {
            "FT_MEMBER_INFO": {
                'field': "household_register_nature",
                'type': int,
                'lower': False,
                'use': True
            }
        },
        "困境数据": {
            "困境数据.xlsx": {
                'field': "户籍性质",
                "type": str,
                "lower": False,
                'use': True
            }
        }
    },
    "emergencyPeople": {
        "yikatong": {
            "丰台老人基础信息.csv": {
                'field': "联系人",
                'type': str,
                'use': True,
                'lower': False
            },
            "服务对象.csv": {
                'field': 'URGENT',
                'type': str,
                'lower': False,
                'use': True
            }
        },
        "shehuifuli": {
            "BA_ORG_MEMBER_INFO": {
                'field': 'emergency',
                'type': str,
                'lower': False,
                'use': True
            }
        },
        "困境数据": {
            "困境数据.xlsx": {
                'field': "联系人",
                "type": str,
                "lower": False,
                'use': True
            }
        }
    },
    "emergencyPhone": {
        "yikatong": {
            "丰台老人基础信息.csv": {
                'field': "联系人电话",
                'type': str,
                'use': True,
                'lower': False
            },
            "服务对象.csv": {
                'field': 'PHONE',
                'type': str,
                'lower': False,
                'use': True
            }
        },
        "shehuifuli": {
            "BA_ORG_MEMBER_INFO": {
                'field': 'telephone',
                'anotherField': 'phone',
                'type': str,
                'lower': False,
                'use': True
            }
        },
        "困境数据": {
            "困境数据.xlsx": {
                'field': "联系电话",
                "type": str,
                "lower": False,
                'use': True
            }
        }
    },
    "pensions_bjtCard": {
        "yikatong": {
            "丰台老人基础信息.csv": {
                'field': "一卡通号",
                'type': str,
                'use': True,
                'lower': False
            },
            "服务对象.csv": {
                'field': 'BJTCARD',
                'type': str,
                'lower': False,
                'use': True
            }
        },
        "困境数据": {
            "困境数据.xlsx": {
                'field': "一卡通",
                "type": str,
                "lower": False,
                'use': True
            }
        }
    },
    "pensions_bankCard": {
        "yikatong": {
            "丰台老人基础信息.csv": {
                'field': "银行卡号",
                'type': str,
                'use': True,
                'lower': False
            },
            "服务对象.csv": {
                'field': 'BANK_CARD',
                'type': str,
                'lower': False,
                'use': True
            }
        },
        "困境数据": {
            "困境数据.xlsx": {
                'field': "银行卡",
                "type": str,
                "lower": False,
                'use': True
            }
        }
    },
    "pensions_insuranceType": {
        "yikatong": {
            "服务对象.csv": {
                'field': 'SUBSIDYMETHOD_TYPE',
                'type': int,
                'lower': False,
                'use': True,
                'yikatongDict': True
            }
        },
        "困境数据": {
            "困境数据.xlsx": {
                'field': "保障对象类型",
                "type": str,
                "lower": False,
                'use': True
            }
        }
    },
    "pensions_medicalInsuranceType": {
        "yikatong": {
            "丰台老人基础信息.csv": {
                'field': "医疗保障类型",
                'type': str,
                'use': True,
                'lower': False
            },
            "服务对象.csv": {
                'field': 'MEDICARETYPE',
                'type': str,
                'lower': False,
                'use': True
            }
        },
        "困境数据": {
            "困境数据.xlsx": {
                'field': "医保类型",
                "type": str,
                "lower": False,
                'use': True
            }
        }
    },
    "pensions_insured": {
        "dibao": {
            "FT_MEMBER_INFO": {
                'field': "member_insurance_flag",
                'type': str,
                'realType': int,
                'lower': False,
                'split': ',',
                'use': True
            }
        },
        "困境数据": {
            "困境数据.xlsx": {
                'field': "参保类型",
                "type": str,
                "lower": False,
                'use': True
            }
        }
    },
    "pensions_mininumLivingLevel": {
        "dibao": {
            "FT_MEMBER_INFO": {
                'field': "member_relief_standard_type",
                'type': int,
                'lower': False,
                'use': True
            }
        },
        "困境数据": {
            "困境数据.xlsx": {
                'field': "保障标准类型",
                "type": str,
                "lower": False,
                'use': True
            }
        }
    },
    "pensions_laborCapacity": {
        "dibao": {
            "FT_MEMBER_INFO": {
                'field': "member_work_ability",
                'type': int,
                'lower': False,
                'use': True
            }
        },
        "困境数据": {
            "困境数据.xlsx": {
                'field': "劳动能力",
                "type": str,
                "lower": False,
                'use': True
            }
        }
    },
    "pensions_employmentStatus": {
        "dibao": {
            "FT_MEMBER_INFO": {
                'field': "member_work_state",
                'type': int,
                'lower': False,
                'use': True
            }
        },
        "困境数据": {
            "困境数据.xlsx": {
                'field': "就业状况",
                "type": str,
                "lower": False,
                'use': True
            }
        }
    },
    "pensions_vocation": {
        "dibao": {
            "FT_MEMBER_INFO": {
                'field': "position",
                'type': int,
                'lower': False,
                'use': True
            }
        },
        "困境数据": {
            "困境数据.xlsx": {
                'field': "职业",
                "type": str,
                "lower": False,
                'use': True
            }
        }
    },
    "pensions_healthStatus": {
        "dibao": {
            "FT_MEMBER_INFO": {
                'field': "member_health_state",
                'type': int,
                'lower': False,
                'use': True
            }
        },
        "困境数据": {
            "困境数据.xlsx": {
                'field': "健康状况",
                "type": str,
                "lower": False,
                'use': True
            }
        }
    },
    "pensions_bodyStatus": {
        "dibao": {
            "FT_MEMBER_INFO": {
                'field': "member_body_state",
                'type': int,
                'lower': False,
                'use': True
            }
        },
        "困境数据": {
            "困境数据.xlsx": {
                'field': "身体状况",
                "type": str,
                "lower": False,
                'use': True
            }
        }
    },
    "pensions_residenceStatus": {
        "yikatong": {
            "丰台老人基础信息.csv": {
                'field': "居住情况",
                'type': str,
                'use': True,
                'lower': False
            },
            "服务对象.csv": {
                'field': 'LIVE',
                'type': str,
                'lower': False,
                'use': True
            }
        },
        "dibao": {
            "FT_MEMBER_INFO": {
                'field': "residence_situation",
                'type': int,
                'lower': False,
                'use': True
            }
        },
        "困境数据": {
            "困境数据.xlsx": {
                'field': "居住状况",
                "type": str,
                "lower": False,
                'use': True
            }
        }
    },
    "pensions_livingDegree": {
        "yikatong": {
            "丰台老人基础信息.csv": {
                'field': "生活自理情况",
                'type': str,
                'use': True,
                'lower': False
            }
        },
        "dibao": {
            "FT_MEMBER_INFO": {
                'field': "member_live_ability",
                'type': int,
                'lower': False,
                'use': True
            }
        },
        "困境数据": {
            "困境数据.xlsx": {
                'field': "自理程度",
                "type": str,
                "lower": False,
                'use': True
            }
        }
    },
    "pensions_careType": {
        "dibao": {
            "FT_MEMBER_INFO": {
                'field': "life_care",
                'type': int,
                'lower': False,
                'use': True
            }
        },
        "困境数据": {
            "困境数据.xlsx": {
                'field': "照顾类型",
                "type": str,
                "lower": False,
                'use': True
            }
        }
    },
    "pensions_economicSource": {
        "yikatong": {
            "丰台老人基础信息.csv": {
                'field': "主要经济来源",
                'type': str,
                'use': True,
                'lower': False
            },
            "服务对象.csv": {
                'field': 'ECONOMICSOURCES',
                'type': str,
                'lower': False,
                'use': True
            }
        },
        "困境数据": {
            "困境数据.xlsx": {
                'field': "经济来源",
                "type": str,
                "lower": False,
                'use': True
            }
        }
    },
    "pensions_incomingLevel": {
        "yikatong": {
            "丰台老人基础信息.csv": {
                'field': "月收入",
                'type': str,
                'use': True,
                'lower': False
            },
            "服务对象.csv": {
                'field': 'INCOMELEVEL',
                'type': str,
                'lower': False,
                'use': True
            }
        },
        "困境数据": {
            "困境数据.xlsx": {
                'field': "收入水平",
                "type": str,
                "lower": False,
                'use': True
            }
        }
    },
    "flag_isObjectServiced": {
        "困境数据": {
            "困境数据.xlsx": {
                'field': "是否困境对象",
                "type": str,
                "lower": False,
                'use': True,
            }
        }
    },
    "flag_isObjectTraditional": {
        "dibao": {
            "FT_MEMBER_INFO": {
                'field': "traditional_object",
                'type': int,
                'lower': False,
                'use': True
            }
        },
        "困境数据": {
            "困境数据.xlsx": {
                'field': "是否传统保障对象",
                "type": str,
                "lower": False,
                'use': True
            }
        }
    },
    "flag_isSpecialSalvation": {
        "dibao": {
            "FT_MEMBER_INFO": {
                'field': "special_object",
                'type': int,
                'lower': False,
                'use': True
            }
        },
        "困境数据": {
            "困境数据.xlsx": {
                'field': "是否特殊救助对象",
                "type": str,
                "lower": False,
                'use': True
            }
        }
    },
    "flag_isLonely": {
        "shehuifuli": {
            "BA_ORG_MEMBER_INFO": {
                'field': 'is_lonely_man',
                'type': int,
                'lower': False,
                'use': True
            }
        },
        "yikatong": {
            "服务对象.csv": {
                'field': 'LIVE',
                'type': str,
                'lower': False,
                'only': "独居",
                'use': True
            }
        },
        "困境数据": {
            "困境数据.xlsx": {
                'field': "是否独居",
                "type": str,
                "lower": False,
                'use': True
            }
        }
    },
    "flag_isDisabled": {
        "shehuifuli": {
            "BA_ORG_MEMBER_INFO": {
                'field': 'sf_disabled',
                'type': int,
                'lower': False,
                'use': True
            }
        },
        "困境数据": {
            "困境数据.xlsx": {
                'field': "是否残疾",
                "type": str,
                "lower": False,
                'use': True
            }
        }
    },
    "flag_isNZJ": {
        "dibao": {
            "FT_MEMBER_INFO": {
                'field': "area_lzj",
                'type': int,
                'lower': False,
                'use': True
            }
        },
        "困境数据": {
            "困境数据.xlsx": {
                'field': "是否农转居",
                "type": str,
                "lower": False,
                'use': True
            }
        }
    },
    "flag_isReleased": {
        "dibao": {
            "FT_MEMBER_INFO": {
                'field': "release_member",
                'type': int,
                'lower': False,
                'use': True
            }
        },
        "困境数据": {
            "困境数据.xlsx": {
                'field': "是否两劳释放人员",
                "type": str,
                "lower": False,
                'use': True
            }
        }
    },
    "flag_isExservicee": {
        "dibao": {
            "FT_MEMBER_INFO": {
                'field': "exserviee_armyman",
                'type': int,
                'lower': False,
                'use': True
            }
        },
        "困境数据": {
            "困境数据.xlsx": {
                'field': "是否退役人员",
                "type": str,
                "lower": False,
                'use': True
            }
        }
    },
    "flag_isReservoirImmigrant": {
        "dibao": {
            "FT_MEMBER_INFO": {
                'field': "reservoir_immigrant",
                'type': int,
                'lower': False,
                'use': True
            }
        },
        "困境数据": {
            "困境数据.xlsx": {
                'field': "是否水库移民",
                "type": str,
                "lower": False,
                'use': True
            }
        }
    },
    "flag_isAbroadRelative": {
        "dibao": {
            "FT_MEMBER_INFO": {
                'field': "overseas_relatives",
                'type': int,
                'lower': False,
                'use': True
            }
        },
        "困境数据": {
            "困境数据.xlsx": {
                'field': "是否海外侨眷",
                "type": str,
                "lower": False,
                'use': True
            }
        }
    },
    "flag_isDeath": {
        "yikatong": {
            "丰台老人基础信息.csv": {
                'field': "是否死亡",
                'type': str,
                'use': True,
                'lower': False
            },
            "服务对象.csv": {
                'field': 'IS_DEATH',
                'type': int,
                'lower': False,
                'use': True,
                'yikatongDict': True
            }
        },
        "dibao": {
            "FT_MEMBER_INFO": {
                'field': "member_status",
                'type': int,
                'lower': False,
                'use': True
            }
        },
        "shehuifuli": {
            "BA_ORG_MEMBER_INFO": {
                'field': 'leave_deatil',
                'type': str,
                'lower': False,
                'use': True,
                'contain': {"死", "逝"}
            }
        },
        "困境数据": {
            "困境数据.xlsx": {
                'field': "是否死亡",
                "type": str,
                "lower": False,
                'use': True
            }
        }
    }
}

# pandas dict
pds = {}
identityCard_index = set()
baseDirct = '/home/ada/PycharmProjects/fengtai_local/all_origin_data'
baseDict = '/home/ada/PycharmProjects/fengtai_local/all_origin_data/dibao/低保_福利-字典值.xlsx'
baseDistrict = '/home/ada/PycharmProjects/fengtai_local/all_origin_data/dibao/区划信息.xlsx'
FT_FAMILY_INFO = '/home/ada/PycharmProjects/fengtai_local/all_origin_data/dibao/FT_FAMILY_INFO'
yikatongDict = {
    'SEX': {
        '1': "男",
        '2': "女"
    },
    'SUBSIDYMETHOD_TYPE': {
        '1': "托底保障对象",
        '2': "困境保障对象",
        '3': "重点保障对象",
        '4': "一般保障对象"
    },
    'IS_DEATH': {
        '1': "死亡",
        '2': "户籍迁出",
        '3': "其它"
    }
}


@decorator
def evaluationTime(fun, *args, **kwargs):
    start = time.time()
    ret = fun(*args, **kwargs)
    print(f"{fun.__name__} use time {time.time()-start}")
    return ret


def str_name(cnt: str):
    """
        字符串处理
    :param cnt:
    :return:
    """
    out = cnt.strip()
    out = out.replace('\u3000', '')
    out = out.replace(' ', '')
    return out


def indexStrip(ar, item):
    cnt = ar[item]
    if type(cnt) == str:
        cnt = str_name(cnt)
    return cnt


def idIndex(path, **kwargs):
    if str(path) not in pds:
        if 'fileType' in kwargs:
            tp = kwargs['fileType']
            if tp == 'json':
                tmp = pd.read_json(str(path), dtype=str)
                field = kwargs['field'].lower() if kwargs['lower'] else kwargs['field']
                tmp[field] = tmp.apply(indexStrip, axis=1, args=(field,))
                pds[str(path)] = tmp.set_index(field)
            elif tp == 'csv':
                tmp = pd.read_csv(str(path), dtype=str)
                field = kwargs['field'].lower() if kwargs['lower'] else kwargs['field']
                tmp[field] = tmp.apply(indexStrip, axis=1, args=(field,))
                pds[str(path)] = tmp.set_index(field)
            elif tp == 'excel':
                tmp = pd.read_excel(str(path), dtype=str)
                field = kwargs['field'].lower() if kwargs['lower'] else kwargs['field']
                tmp[field] = tmp.apply(indexStrip, axis=1, args=(field,))
                pds[str(path)] = tmp.set_index(field)
    if not kwargs['use']:
        return None
    mp = set(string.digits + string.ascii_letters)
    for val in pds[str(path)].index:
        # val = data.iloc[i]
        if type(val) == str:
            x = val.strip()
            if all([i in mp for i in x]):
                identityCard_index.add(x)


@evaluationTime
def getBasicIndex(od: OrderedDict, key="identityCard"):
    for k, v in od.items():
        if k != key:
            continue
        for db, db_v in v.items():
            if not isinstance(db_v, dict):
                raise Exception(f"the value is not dict {db_v}")
            for table, table_v in db_v.items():
                idIndex(pathlib.Path(os.path.join(baseDirct, db, table)), **table_v)
    return identityCard_index


def detailItem(id_, path, ids_table_v, table_v):
    if not table_v['use']:
        print(f"the field is not use {table_v['field']} {id_}")
        return None
    out = []
    # print(f"the id_ is {id_} table_v {table_v}")
    ids_field = ids_table_v['field'].lower() if ids_table_v['lower'] else ids_table_v['field']
    fields = []
    fieldOne = table_v['field'].lower() if table_v['lower'] else table_v['field']
    fields.append(fieldOne)
    if 'anotherField' in table_v:
        anotherField = table_v['anotherField'].lower if table_v['lower'] else table_v['anotherField']
        fields.append(anotherField)
    # print(f"the ids_field {ids_field} field {field} id {id_}, {str(path)}")
    df_ = pds[str(path)]
    if id_ not in df_.index:
        return None
    for field in fields:
        data = df_.loc[id_,]
        if type(data) == pandas.core.series.Series:
            data = [data[field], ]
        else:
            data = data[field]
        for cnt in data:
            if isinstance(cnt, str):
                if cnt.isspace() or cnt == 'None' or cnt == 'nan':
                    continue
            elif cnt in {np.NaN, np.NAN, np.nan}:
                continue
            # print(f"value is {cnt} field_type:{table_v['type']}  value_type:{type(cnt)} id:{id_}")
            if table_v['type'] == int:
                # 服务对象里的字典
                if 'yikatongDict' in table_v and table_v['yikatongDict']:
                    if cnt not in yikatongDict[field]:
                        continue
                    out.append(yikatongDict[field][cnt])
                    continue
                if 'baseDict' in table_v and not table_v['baseDict']:
                    df = pds['baseDistrict']
                else:
                    df = pds['baseDict']
                # df = df[df['ID'] == int(float(cnt))]
                index = "{}".format(int(float(cnt)))
                if index not in df.index:
                    continue
                df = df.loc[index,]
                if type(df) == pandas.core.series.Series:
                    df_list = [df['NAME'], ]
                else:
                    df_list = df['NAME']
                for i in df_list:
                    out.append(i)
                # print(f"the last result is11  {cnt}")
            elif table_v['type'] == str:
                flag = False
                if 'contain' in table_v:
                    if cnt in table_v['contain']:
                        out.append("死亡")
                    flag = True
                # 参加保障
                if 'split' in table_v:
                    ar = cnt.split(table_v['split'])
                    if 'realType' in table_v and table_v['realType'] == int:
                        for r in ar:
                            if not r:
                                continue
                            df = pds['baseDict']
                            index = r
                            if index not in df.index:
                                continue
                            df = df.loc[index,]
                            if type(df) == pandas.core.series.Series:
                                df_list = [df['NAME'], ]
                            else:
                                df_list = df['NAME']
                            for i in df_list:
                                out.append(i)
                            flag = True
                # locate信息
                if 'relateTable' in table_v:
                    # if table_v['relateTable'] not in pds:
                    #     if table_v['relateFileType'] == 'json':
                    #         tmp = pd.read_json(os.path.join(os.path.dirname(str(path)), table_v['relateTable']), dtype=str)
                    #         pds[table_v['relateTable']] = tmp.set_index(field)
                    relate_df = pds[table_v['relateTable']]
                    if cnt not in relate_df.index:
                        continue
                    data = relate_df.loc[cnt,]
                    relateField = table_v['relateField'].lower() if table_v['relateFieldLower'] else table_v[
                        'relateField']
                    if type(data) == pandas.core.series.Series:
                        set_ = [data[relateField], ]
                    else:
                        set_ = data[relateField]
                    for x in set_:
                        df = pds['baseDistrict']
                        # df = df[df['ID'] == int(float(cnt))]
                        index = str(int(float(x)))
                        if index not in df.index:
                            continue
                        df = df.loc[index,]
                        if type(df) == pandas.core.series.Series:
                            df_list = [df['NAME'], ]
                        else:
                            df_list = df['NAME']
                        for i in df_list:
                            out.append(i)
                    flag = True
                if not flag:
                    out.append(cnt)
                # print(f"the last result is22  {cnt}")
    if out:
        return [*reversed([i for i in out]), ]
    else:
        return None


def field_process(id_, od, key):
    ids_set = od['identityCard']
    result = []
    if key == 'birthDay':
        val = id_[6:14]
        result.append(val)
        return result
    for k, v in od.items():
        if key != k:
            continue
        for db, db_v in v.items():
            for table, table_v in db_v.items():
                for ids_db, ids_db_v in ids_set.items():
                    if ids_db != db:
                        continue
                    for ids_table, ids_table_v in ids_db_v.items():
                        if ids_table != table:
                            continue
                        path = pathlib.Path(os.path.join(baseDirct, db, table))
                        # print(f"the ids_table {ids_db} {db}  {ids_table} {str(path)}")
                        ret = detailItem(id_, path, ids_table_v, table_v)
                        if ret:
                            for i in ret:
                                result.append(i)
        break
    if result:
        return result
    else:
        return None


@evaluationTime
def test():
    """
        主入口
    :return:
    """
    tmp = pd.read_excel(baseDict, dtype=str)
    pds['baseDict'] = tmp.set_index('ID')
    tmp = pd.read_excel(baseDistrict, dtype=str)
    pds['baseDistrict'] = tmp.set_index('ID')
    tmp = pd.read_json(FT_FAMILY_INFO, dtype=str)
    pds['FT_FAMILY_INFO'] = tmp.set_index('jzh_sq_salvage_info_id')
    od = OrderedDict(fields)
    ids = getBasicIndex(od)
    ids = sorted(ids)
    print(f"the len of ids is {len(ids)}")
    index = 0
    ds = []
    set_num = 10
    with ProcessPoolExecutor(max_workers=4) as executor:
        with open('out_new.txt', 'w') as fd:
            for id_ in ids:
                dt = Detail()
                dt.identityCard = id_
                for atr in od.keys():
                    if atr == 'identityCard':
                        continue
                    setattr(dt, atr, executor.submit(field_process, id_, od, atr))
                ds.append(dt)
                index += 1

                if index % set_num == 0:
                    for item in ds:
                        for k in vars(item):
                            if k == 'identityCard':
                                continue
                            setattr(item, k, getattr(item, k).result())
                        fd.write("{}\n".format(item.toJson))
                    del ds[:]
                print(f"---      ------------  index {index}")
                # if index == 50:
                #     break

            for item in ds:
                for k in vars(item):
                    if k == 'identityCard':
                        continue
                    setattr(item, k, getattr(item, k).result())
                fd.write("{}\n".format(item.toJson))


def jsonToExcel():
    src = 'out_new.txt'
    ds = []
    with open(src, 'r') as fd:
        index = 0
        for line in fd.readlines().__iter__():
            jdata = ast.literal_eval(line)
            if index == 0:
                keys = [*jdata.keys(), ]
            index += 1
            ds.append(jdata)
    df = pd.DataFrame(ds, columns=keys)
    df.to_excel('out_new.xlsx')


multiVal = []


def columnsVal(ar, *args):
    dt = Detail()
    flag = False
    for k in args:
        val = ar[k]
        if k == 'identityCard':
            setattr(dt, 'identityCard', val)
        if type(val) == str and ',' in val:
            setattr(dt, k, val)
            flag = True
    if flag:
        multiVal.append(dt)


def getMultiValue():
    df = pd.read_excel('out_new.xlsx', dtype=str)
    df.apply(columnsVal, axis=1, args=[k for k in df.columns][1:])
    names = set()
    with open('multi_val.txt', 'w') as fd:
        multi = sorted(multiVal, key=lambda x: getattr(x, 'identityCard'))
        for it in multi:
            for k in vars(it):
                names.add(k)
            fd.write("{}\n".format(it.toJson))
        names = sorted(names)
        fd.write("duplicate name {}\n".format(names))


if __name__ == '__main__':
    test()
