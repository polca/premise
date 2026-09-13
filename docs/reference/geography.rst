Country-to-IAM region mapping
===============================

.. contents:: On this page
   :local:
   :depth: 1

IAM models have slightly different geographical resolutions and definitions.


Map of IMAGE regions

.. image:: /map_image.png
   :width: 500pt
   :align: center
   :alt: Map of IMAGE regions

Map of REMIND regions

.. image:: /map_remind_regions.png
   :width: 500pt
   :align: center
   :alt: Map of REMIND regions

Map of REMIND-EU regions

.. image:: /map_remind_eu_regions.png
   :width: 500pt
   :align: center
   :alt: Map of REMIND-EU regions

Map of TIAM-UCL regions

.. image:: /map_tiam_ucl_regions.png
   :width: 500pt
   :align: center
   :alt: Map of TIAM-UCL regions

Map of GCAM regions

.. image:: /map_gcam_regions.png
   :width: 500pt
   :align: center
   :alt: Map of GCAM regions

Map of MESSAGE regions

.. image:: /map_message_regions.png
   :width: 500pt
   :align: center
   :alt: Map of MESSAGE regions


*premise* uses the following correspondence between ecoinvent locations
and IAM regions. This mapping is performed by the `constructive_geometries <https://github.com/cmutel/constructive_geometries>`__
implementation in the `wurst <https://github.com/polca/wurst>`__ library.

 =============== ================================= ================================ ======================== =========================== ================ ========================
  Country Code    message                           gcam                             tiam-ucl                 remind                      remind-eu        image
 =============== ================================= ================================ ======================== =========================== ================ ========================
  AE              MEA                               Middle East                      MEA                      MEA                         MEA              ME
  AF              SAS                               South Asia                       ODA                      OAS                         OAS              RSAS
  AG              LAM                               Central America and Caribbean    CSA                      LAM                         LAM              N/A
  AI              LAM                               Central America and Caribbean    CSA                      LAM                         LAM              RCAM
  AL              EEU                               Europe_Non_EU                    WEU                      NEU                         NES              CEU
  AM              FSU                               Central Asia                     FSU                      REF                         REF              RUS
  AO              AFR                               Africa_Southern                  AFR                      SSA                         SSA              RSAF
  AQ              MEA                               Middle East                      MEA                      MEA                         MEA              ME
  AR              LAM                               Argentina                        CSA                      LAM                         LAM              RSAM
  AS              PAS                               Southeast Asia                   ODA                      OAS                         OAS              OCE
  AT              WEU                               EU-15                            WEU                      EUR                         EWN              WEU
  AU              PAO                               Australia_NZ                     AUS                      CAZ                         CAZ              OCE
  AZ              FSU                               Central Asia                     FSU                      REF                         REF              RUS
  BA              EEU                               Europe_Non_EU                    EEU                      NEU                         NES              CEU
  BD              SAS                               South Asia                       ODA                      OAS                         OAS              RSAS
  BE              WEU                               EU-15                            WEU                      EUR                         EWN              WEU
  BF              AFR                               Africa_Western                   AFR                      SSA                         SSA              WAF
  BG              EEU                               EU-12                            EEU                      EUR                         ECS              CEU
  BH              MEA                               Middle East                      MEA                      MEA                         MEA              ME
  BI              AFR                               Africa_Eastern                   AFR                      SSA                         SSA              EAF
  BJ              AFR                               Africa_Western                   AFR                      SSA                         SSA              WAF
  BN              PAS                               Southeast Asia                   MEA                      OAS                         OAS              SEAS
  BO              LAM                               South America_Southern           CSA                      LAM                         LAM              RSAM
  BR              LAM                               Brazil                           CSA                      LAM                         LAM              BRA
  BS              LAM                               Central America and Caribbean    CSA                      LAM                         LAM              RCAM
  BT              SAS                               South Asia                       ODA                      OAS                         OAS              RSAS
  BW              AFR                               Africa_Southern                  AFR                      SSA                         SSA              RSAF
  BY              FSU                               Europe_Eastern                   FSU                      REF                         REF              UKR
  BZ              LAM                               Central America and Caribbean    CSA                      LAM                         LAM              RCAM
  CA              NAM                               Canada                           CAN                      CAZ                         CAZ              CAN
  CD              AFR                               Africa_Western                   AFR                      SSA                         SSA              WAF
  CF              AFR                               Africa_Western                   AFR                      SSA                         SSA              WAF
  CG              AFR                               Africa_Western                   AFR                      SSA                         SSA              WAF
  CH              WEU                               European Free Trade Association  WEU                      NEU                         NEN              WEU
  CI              AFR                               Africa_Western                   AFR                      SSA                         SSA              WAF
  CL              LAM                               South America_Southern           CSA                      LAM                         LAM              RSAM
  CM              AFR                               Africa_Western                   AFR                      SSA                         SSA              WAF
  CN              CHN                               China                            CHI                      CHA                         CHA              CHN
  CO              LAM                               Colombia                         CSA                      LAM                         LAM              RSAM
  CR              LAM                               Central America and Caribbean    CSA                      LAM                         LAM              RCAM
  CU              LAM                               Central America and Caribbean    CSA                      LAM                         LAM              RCAM
  CY              WEU                               EU-12                            MEA                      EUR                         ESC              N/A
  CZ              EEU                               EU-12                            EEU                      EUR                         ECE              CEU
  DE              WEU                               EU-15                            WEU                      EUR                         DEU              WEU
  DJ              AFR                               Africa_Eastern                   AFR                      SSA                         SSA              EAF
  DK              WEU                               EU-15                            WEU                      EUR                         ENC              WEU
  DM              LAM                               Central America and Caribbean    CSA                      LAM                         LAM              RCAM
  DO              LAM                               Central America and Caribbean    CSA                      LAM                         LAM              RCAM
  DZ              MEA                               Africa_Northern                  AFR                      MEA                         MEA              NAF
  EC              LAM                               South America_Southern           CSA                      LAM                         LAM              RSAM
  EE              EEU                               EU-12                            FSU                      EUR                         ECE              CEU
  EG              MEA                               Africa_Northern                  AFR                      MEA                         MEA              NAF
  EH              AFR                               Africa_Northern                  AFR                      SSA                         SSA              NAF
  ER              AFR                               Africa_Eastern                   AFR                      SSA                         SSA              EAF
  ES              WEU                               EU-15                            WEU                      EUR                         ESW              WEU
  ET              AFR                               Africa_Eastern                   AFR                      SSA                         SSA              EAF
  FI              WEU                               EU-15                            WEU                      EUR                         ENC              WEU
  FJ              PAS                               Southeast Asia                   ODA                      OAS                         OAS              OCE
  FK              LAM                               South America_Southern           CSA                      LAM                         LAM              RSAM
  FR              WEU                               EU-15                            WEU                      EUR                         FRA              WEU
  GA              AFR                               Africa_Western                   AFR                      SSA                         SSA              WAF
  GB              WEU                               EU-15                            UK                       EUR                         UKI              WEU
  GE              FSU                               Central Asia                     FSU                      REF                         REF              RUS
  GF              LAM                               South America_Northern           CSA                      LAM                         LAM              RSAM
  GH              AFR                               Africa_Western                   AFR                      SSA                         SSA              WAF
  GI              WEU                               EU-15                            WEU                      EUR                         UKI              WEU
  GL              WEU                               EU-15                            NEU                      NEU                         NEN              WEU
  GM              AFR                               Africa_Western                   AFR                      SSA                         SSA              WAF
  GN              AFR                               Africa_Western                   AFR                      SSA                         SSA              WAF
  GQ              AFR                               Africa_Western                   AFR                      SSA                         SSA              WAF
  GR              WEU                               EU-15                            WEU                      EUR                         ESC              WEU
  GT              LAM                               Central America and Caribbean    CSA                      LAM                         LAM              RCAM
  GW              AFR                               Africa_Western                   AFR                      SSA                         SSA              WAF
  GY              LAM                               South America_Northern           CSA                      LAM                         LAM              RSAM
  HK              CHN                               China                            CHI                      CHA                         CHA              CHN
  HN              LAM                               Central America and Caribbean    CSA                      LAM                         LAM              RCAM
  HR              EEU                               Europe_Non_EU                    EEU                      EUR                         ECS              CEU
  HT              LAM                               Central America and Caribbean    CSA                      LAM                         LAM              RCAM
  HU              EEU                               EU-12                            EEU                      EUR                         ECS              CEU
  ID              PAS                               Indonesia                        ODA                      OAS                         OAS              INDO
  IE              WEU                               EU-15                            WEU                      EUR                         UKI              WEU
  IL              MEA                               Middle East                      MEA                      MEA                         MEA              ME
  IN              SAS                               India                            IND                      IND                         IND              INDIA
  IQ              MEA                               Middle East                      MEA                      MEA                         MEA              ME
  IR              MEA                               Middle East                      MEA                      MEA                         MEA              ME
  IS              WEU                               European Free Trade Association  WEU                      NEU                         NEN              WEU
  IT              WEU                               EU-15                            WEU                      EUR                         ESC              WEU
  JM              LAM                               Central America and Caribbean    CSA                      LAM                         LAM              RCAM
  JO              MEA                               Middle East                      MEA                      MEA                         MEA              ME
  JP              PAO                               Japan                            JPN                      JPN                         JPN              JAP
  KE              AFR                               Africa_Eastern                   AFR                      SSA                         SSA              EAF
  KG              FSU                               Central Asia                     FSU                      REF                         REF              STAN
  KH              RCPA                              Southeast Asia                   ODA                      OAS                         OAS              SEAS
  KI              PAS                               Southeast Asia                   ODA                      OAS                         OAS              OCE
  KM              AFR                               Africa_Eastern                   AFR                      SSA                         SSA              EAF
  KN              LAM                               Central America and Caribbean    CSA                      LAM                         LAM              RCAM
  KP              RCPA                              Southeast Asia                   ODA                      OAS                         OAS              KOR
  KR              PAS                               South Korea                      SKO                      OAS                         OAS              KOR
  KW              MEA                               Middle East                      MEA                      MEA                         MEA              ME
  KY              LAM                               Central America and Caribbean    CSA                      LAM                         LAM              RCAM
  KZ              FSU                               Central Asia                     FSU                      REF                         REF              STAN
  LA              RCPA                              Southeast Asia                   ODA                      OAS                         OAS              SEAS
  LB              MEA                               Middle East                      MEA                      MEA                         MEA              ME
  LC              LAM                               Central America and Caribbean    CSA                      LAM                         LAM              RCAM
  LI              WEU                               EU-15                            WEU                      NEU                         NEN              WEU
  LK              SAS                               South Asia                       ODA                      OAS                         OAS              RSAS
  LR              AFR                               Africa_Western                   AFR                      SSA                         SSA              WAF
  LS              AFR                               Africa_Southern                  AFR                      SSA                         SSA              RSAF
  LT              EEU                               EU-12                            FSU                      EUR                         ECE              CEU
  LU              WEU                               EU-15                            WEU                      EUR                         EWN              WEU
  LV              EEU                               EU-12                            FSU                      EUR                         ECE              CEU
  LY              MEA                               Africa_Northern                  AFR                      MEA                         MEA              NAF
  MA              MEA                               Africa_Northern                  AFR                      MEA                         MEA              NAF
  MC              WEU                               EU-15                            WEU                      NEU                         NES              WEU
  MD              FSU                               Europe_Eastern                   FSU                      REF                         REF              UKR
  ME              EEU                               Europe_Non_EU                    EEU                      NEU                         NES              CEU
  MG              AFR                               Africa_Eastern                   AFR                      SSA                         SSA              RSAF
  MK              EEU                               Europe_Non_EU                    EEU                      NEU                         NES              CEU
  ML              AFR                               Africa_Western                   AFR                      SSA                         SSA              WAF
  MM              PAS                               Southeast Asia                   ODA                      OAS                         OAS              SEAS
  MN              RCPA                              Central Asia                     ODA                      OAS                         OAS              CHN
  MO              CHN                               China                            CHI                      CHA                         CHA              CHN
  MR              AFR                               Africa_Western                   AFR                      SSA                         SSA              WAF
  MS              LAM                               Central America and Caribbean    CSA                      LAM                         LAM              RCAM
  MT              WEU                               EU-12                            WEU                      EUR                         ESC              WEU
  MU              AFR                               Africa_Eastern                   ODA                      SSA                         SSA              EAF
  MW              AFR                               Africa_Southern                  AFR                      SSA                         SSA              RSAF
  MX              LAM                               Mexico                           MEX                      MEX                         LAM              MEX
  MY              PAS                               Southeast Asia                   ODA                      OAS                         OAS              SEAS
  MZ              AFR                               Africa_Southern                  AFR                      SSA                         SSA              RSAF
  NA              AFR                               Africa_Southern                  AFR                      SSA                         SSA              RSAF
  NC              PAS                               Southeast Asia                   ODA                      OAS                         OAS              OCE
  NE              AFR                               Africa_Western                   AFR                      SSA                         SSA              WAF
  NG              AFR                               Africa_Western                   AFR                      SSA                         SSA              WAF
  NI              LAM                               Central America and Caribbean    CSA                      LAM                         LAM              RCAM
  NL              WEU                               EU-15                            WEU                      EUR                         EWN              WEU
  NO              WEU                               European Free Trade Association  WEU                      NEU                         NEN              WEU
  NP              SAS                               South Asia                       ODA                      OAS                         OAS              RSAS
  NR              PAS                               Southeast Asia                   ODA                      OAS                         OAS              OCE
  NU              PAS                               Southeast Asia                   ODA                      OAS                         OAS              OCE
  NZ              PAO                               Australia_NZ                     AUS                      CAZ                         CAZ              OCE
  OM              MEA                               Middle East                      MEA                      MEA                         MEA              ME
  PA              LAM                               Central America and Caribbean    CSA                      LAM                         LAM              RCAM
  PE              LAM                               South America_Southern           CSA                      LAM                         LAM              RSAM
  PF              PAS                               Southeast Asia                   ODA                      OAS                         OAS              OCE
  PG              PAS                               Southeast Asia                   ODA                      OAS                         OAS              INDO
  PH              PAS                               Southeast Asia                   ODA                      OAS                         OAS              SEAS
  PK              SAS                               Pakistan                         ODA                      OAS                         OAS              RSAS
  PL              EEU                               EU-12                            EEU                      EUR                         ECE              CEU
  PR              LAM                               Central America and Caribbean    CSA                      LAM                         LAM              RCAM
  PS              MEA                               Middle East                      MEA                      MEA                         MEA              ME
  PT              WEU                               EU-15                            WEU                      EUR                         ESW              WEU
  PY              LAM                               South America_Southern           CSA                      LAM                         LAM              RSAM
  QA              MEA                               Middle East                      MEA                      MEA                         MEA              ME
  RE              AFR                               Africa_Eastern                   AFR                      SSA                         SSA              EAF
  RO              EEU                               EU-12                            EEU                      EUR                         ECS              CEU
  RS              EEU                               Europe_Non_EU                    EEU                      NEU                         NES              CEU
  RU              FSU                               Europe_Eastern                   FSU                      REF                         REF              RUS
  RW              AFR                               Africa_Eastern                   AFR                      SSA                         SSA              EAF
  SA              MEA                               Middle East                      MEA                      MEA                         MEA              ME
  SB              PAS                               Southeast Asia                   ODA                      OAS                         OAS              OCE
  SC              AFR                               Africa_Eastern                   AFR                      SSA                         SSA              EAF
  SD              MEA                               Africa_Eastern                   AFR                      MEA                         MEA              EAF
  SE              WEU                               EU-15                            WEU                      EUR                         ENC              WEU
  SG              PAS                               Southeast Asia                   ODA                      OAS                         OAS              SEAS
  SH              AFR                               Africa_Western                   AFR                      SSA                         SSA              WAF
  SI              EEU                               EU-12                            EEU                      EUR                         ECS              CEU
  SK              EEU                               EU-12                            EEU                      EUR                         ECE              CEU
  SL              AFR                               Africa_Western                   AFR                      SSA                         SSA              WAF
  SM              WEU                               EU-15                            WEU                      NEU                         NES              WEU
  SN              AFR                               Africa_Western                   AFR                      SSA                         SSA              WAF
  SO              AFR                               Africa_Eastern                   AFR                      SSA                         SSA              EAF
  SR              LAM                               South America_Northern           CSA                      LAM                         LAM              RSAM
  SS              AFR                               Africa_Eastern                   AFR                      SSA                         SSA              EAF
  ST              AFR                               Africa_Western                   AFR                      SSA                         SSA              WAF
  SV              LAM                               Central America and Caribbean    CSA                      LAM                         LAM              RCAM
  SY              MEA                               Middle East                      MEA                      MEA                         MEA              ME
  SZ              AFR                               Africa_Southern                  AFR                      SSA                         SSA              RSAF
  TC              LAM                               Central America and Caribbean    CSA                      LAM                         LAM              RCAM
  TD              AFR                               Africa_Western                   AFR                      SSA                         SSA              WAF
  TF              MEA                               Africa_Northern                  AFR                      MEA                         MEA              NAF
  TG              AFR                               Africa_Western                   AFR                      SSA                         SSA              WAF
  TH              PAS                               Southeast Asia                   ODA                      OAS                         OAS              SEAS
  TJ              FSU                               Central Asia                     FSU                      REF                         REF              STAN
  TL              PAS                               Southeast Asia                   ODA                      OAS                         OAS              INDO
  TM              FSU                               Central Asia                     FSU                      REF                         REF              STAN
  TN              MEA                               Africa_Northern                  AFR                      MEA                         MEA              NAF
  TO              PAS                               Southeast Asia                   ODA                      OAS                         OAS              OCE
  TR              WEU                               EU-15                            MEA                      MEA                         NES              TUR
  TT              LAM                               Central America and Caribbean    CSA                      LAM                         LAM              RCAM
  TV              PAS                               Southeast Asia                   ODA                      OAS                         OAS              OCE
  TW              PAS                               Southeast Asia                   ODA                      OAS                         OAS              OCE
  TZ              AFR                               Africa_Southern                  AFR                      SSA                         SSA              RSAF
  UA              FSU                               Europe_Eastern                   FSU                      REF                         REF              UKR
  UG              AFR                               Africa_Eastern                   AFR                      SSA                         SSA              EAF
  US              NAM                               USA                              USA                      USA                         USA              USA
  UY              LAM                               South America_Southern           CSA                      LAM                         LAM              RSAM
  UZ              FSU                               Central Asia                     FSU                      REF                         REF              STAN
  VC              LAM                               Central America and Caribbean    CSA                      LAM                         LAM              RCAM
  VE              LAM                               South America_Northern           CSA                      LAM                         LAM              RSAM
  VG              N/A                               N/A                              ---                      LAM                         LAM              RCAM
  VI              NAM                               Central America and Caribbean    CSA                      LAM                         LAM              RCAM
  VN              RCPA                              Southeast Asia                   ODA                      OAS                         OAS              SEAS
  VU              PAS                               Southeast Asia                   ODA                      OAS                         OAS              OCE
  XK              EU                                Europe_Non_EU                    EEU                      NEU                         NES              CEU
  YE              MEA                               Middle East                      MEA                      MEA                         MEA              ME
  ZA              AFR                               South Africa                     AFR                      SSA                         SSA              SAF
  ZM              AFR                               Africa_Southern                  AFR                      SSA                         SSA              RSAF
  ZW              AFR                               Africa_Southern                  AFR                      SSA                         SSA              RSAF
 =============== ================================= ================================ ======================== =========================== ================ ========================


The mapping between ecoinvent locations and IAM regions is available under the following directory:
https://github.com/polca/premise/tree/76dbf845ef73bb765024dda1143960a24964a5fe/premise/iam_variables_mapping/topologies
