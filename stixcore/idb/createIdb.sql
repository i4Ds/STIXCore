CREATE TABLE `A_User_Config_Table_TC_Is_Referenced`
 (
       `Type`                  int,
       `SubType`                       int
);
CREATE TABLE `CAF`
 (
       `CAF_NUMBR`                     varchar (20) NOT NULL,
       `CAF_DESCR`                     varchar (64) NOT NULL,
       `CAF_ENGFMT`                    varchar (2) NOT NULL,
       `CAF_RAWFMT`                    varchar (2) NOT NULL,
       `CAF_RADIX`                     varchar (2),
       `CAF_UNIT`                      varchar (8),
       `CAF_NCURVE`                    int,
       `SDB_IMPORTED`                  char NOT NULL,
       `Originator`                    varchar (20)
);
CREATE TABLE `CAP`
 (
       `CAP_NUMBR`                     varchar (20) NOT NULL,
       `CAP_XVALS`                     varchar (28) NOT NULL,
       `CAP_YVALS`                     varchar (28) NOT NULL,
       `SDB_IMPORTED`                  char NOT NULL,
       `Originator`                    varchar (20)
);
CREATE TABLE `CCA`
 (
       `CCA_NUMBR`                     varchar (20) NOT NULL,
       `CCA_DESCR`                     varchar (48) NOT NULL,
       `CCA_ENGFMT`                    varchar (2),
       `CCA_RAWFMT`                    varchar (2),
       `CCA_RADIX`                     varchar (2),
       `CCA_UNIT`                      varchar (8),
       `CCA_NCURVE`                    int,
       `SDB_IMPORTED`                  char NOT NULL,
       `Originator`                    varchar (20)
);
CREATE TABLE `CCF`
 (
       `CCF_CNAME`                     varchar (16) NOT NULL,
       `CCF_DESCR`                     varchar (80) NOT NULL,
       `CCF_DESCR2`                    varchar (128) NOT NULL,
       `CCF_CTYPE`                     varchar (16),
       `CCF_CRITICAL`                  varchar (2),
       `CCF_PKTID`                     varchar (16) NOT NULL,
       `CCF_TYPE`                      int,
       `CCF_STYPE`                     int,
       `CCF_APID`                      int,
       `CCF_NPARS`                     int,
       `CCF_PLAN`                      varchar (2),
       `CCF_EXEC`                      varchar (2),
       `CCF_ILSCOPE`                   varchar (2),
       `CCF_ILSTAGE`                   varchar (2),
       `CCF_SUBSYS`                    int,
       `CCF_HIPRI`                     varchar (2),
       `CCF_MAPID`                     int,
       `CCF_DEFSET`                    varchar (16),
       `CCF_RAPID`                     int,
       `CCF_ACK`                       int,
       `CCF_SUBSCHEDID`                        int,
       `SDB_IMPORTED`                  char NOT NULL,
       `Originator`                    varchar (20)
);
CREATE TABLE `CCS`
 (
       `CCS_NUMBR`                     varchar (20) NOT NULL,
       `CCS_XVALS`                     varchar (34) NOT NULL,
       `CCS_YVALS`                     varchar (34) NOT NULL,
       `SDB_IMPORTED`                  char NOT NULL,
       `Originator`                    varchar (20)
);
CREATE TABLE `CDF`
 (
       `CDF_CNAME`                     varchar (16) NOT NULL,
       `CDF_ELTYPE`                    varchar (2) NOT NULL,
       `CDF_DESCR`                     varchar (48),
       `CDF_ELLEN`                     int NOT NULL,
       `CDF_BIT`                       int NOT NULL,
       `CDF_GRPSIZE`                   int,
       `CDF_PNAME`                     varchar (16),
       `CDF_INTER`                     varchar (2),
       `CDF_VALUE`                     varchar (34),
       `CDF_TMID`                      varchar (16),
       `SDB_IMPORTED`                  char NOT NULL,
       `Originator`                    varchar (20)
);
CREATE TABLE `CPC`
 (
       `CPC_PNAME`                     varchar (16) NOT NULL,
       `CPC_DESCR`                     varchar (48) NOT NULL,
       `CPC_PTC`                       int NOT NULL,
       `CPC_PFC`                       int NOT NULL,
       `CPC_DISPFMT`                   varchar (2),
       `CPC_RADIX`                     varchar (2),
       `CPC_UNIT`                      varchar (8),
       `CPC_CATEG`                     varchar (2),
       `CPC_PRFREF`                    varchar (20),
       `CPC_CCAREF`                    varchar (20),
       `CPC_PAFREF`                    varchar (20),
       `CPC_INTER`                     varchar (2),
       `CPC_DEFVAL`                    varchar (34),
       `SDB_IMPORTED`                  char NOT NULL,
       `Originator`                    varchar (20)
);
CREATE TABLE `CPS`
 (
       `CPS_NAME`                      varchar (16) NOT NULL,
       `CPS_PAR`                       varchar (16) NOT NULL,
       `CPS_BIT`                       int NOT NULL,
       `SDB_IMPORTED`                  char NOT NULL,
       `Originator`                    varchar (20)
);
CREATE TABLE `CSF`
 (
       `CSF_NAME`                      varchar (16) NOT NULL,
       `CSF_DESC`                      varchar (48) NOT NULL,
       `CSF_DESC2`                     varchar (128),
       `CSF_IFTT`                      varchar (2),
       `CSF_NFPARS`                    int,
       `CSF_ELEMS`                     int,
       `CSF_CRITICAL`                  varchar (2),
       `CSF_PLAN`                      varchar (2),
       `CSF_EXEC`                      varchar (2),
       `CSF_SUBSYS`                    int,
       `CSF_GENTIME`                   varchar (34),
       `CSF_DOCNAME`                   varchar (64),
       `CSF_ISSUE`                     varchar (20),
       `CSF_DATE`                      varchar (34),
       `CSF_DEFSET`                    varchar (16),
       `CSF_SUBSCHEDID`                        int,
       `SDB_IMPORTED`                  char NOT NULL,
       `Originator`                    varchar (20)
);
CREATE TABLE `CSP`
 (
       `CSP_SQNAME`                    varchar (16) NOT NULL,
       `CSP_FPNAME`                    varchar (16) NOT NULL,
       `CSP_FPNUM`                     int NOT NULL,
       `CSP_DESCR`                     varchar (48),
       `CSP_PTC`                       int NOT NULL,
       `CSP_PFC`                       int NOT NULL,
       `CSP_DISPFMT`                   varchar (2),
       `CSP_RADIX`                     varchar (2),
       `CSP_TYPE`                      varchar (2) NOT NULL,
       `CSP_VTYPE`                     varchar (2),
       `CSP_DEFVAL`                    varchar (34),
       `CSP_CATEG`                     varchar (2),
       `CSP_PRFREF`                    varchar (20),
       `CSP_CCAREF`                    varchar (20),
       `CSP_PAFREF`                    varchar (20),
       `CSP_UNIT`                      varchar (8),
       `SDB_IMPORTED`                  char NOT NULL,
       `Originator`                    varchar (20)
);
CREATE TABLE `CSS`
 (
       `CSS_SQNAME`                    varchar (16) NOT NULL,
       `CSS_COMM`                      varchar (64),
       `CSS_ENTRY`                     int NOT NULL,
       `CSS_TYPE`                      varchar (2) NOT NULL,
       `CSS_ELEMID`                    varchar (16),
       `CSS_NPARS`                     int,
       `CSS_MANDISP`                   varchar (2),
       `CSS_RELTYPE`                   varchar (2),
       `CSS_RELTIME`                   varchar (16),
       `CSS_EXTIME`                    varchar (34),
       `CSS_PREVREL`                   varchar (2),
       `CSS_GROUP`                     varchar (2),
       `CSS_BLOCK`                     varchar (2),
       `CSS_ILSCOPE`                   varchar (2),
       `CSS_ILSTAGE`                   varchar (2),
       `CSS_DYNPTV`                    varchar (2),
       `CSS_STAPTV`                    varchar (2),
       `CSS_CEV`                       varchar (2),
       `SDB_IMPORTED`                  char NOT NULL,
       `Originator`                    varchar (20)
);
CREATE TABLE `CUR`
 (
       `CUR_PNAME`                     varchar (16) NOT NULL,
       `CUR_POS`                       int NOT NULL,
       `CUR_RLCHK`                     varchar (16) NOT NULL,
       `CUR_VALPAR`                    int NOT NULL,
       `CUR_SELECT`                    varchar (20) NOT NULL
);
CREATE TABLE `CVE`
 (
       `CVE_CVSID`                     int NOT NULL,
       `CVE_PARNAM`                    varchar (16) NOT NULL,
       `CVE_INTER`                     varchar (2),
       `CVE_VAL`                       varchar (34),
       `CVE_TOL`                       varchar (34),
       `CVE_CHECK`                     varchar (2),
       `SDB_IMPORTED`                  char NOT NULL,
       `Originator`                    varchar (20)
);
CREATE TABLE `CVS`
 (
       `CVS_ID`                        int NOT NULL,
       `CVS_TYPE`                      varchar (2) NOT NULL,
       `CVS_SOURCE`                    varchar (2) NOT NULL,
       `CVS_START`                     int NOT NULL,
       `CVS_INTERVAL`                  int,
       `CVS_SPID`                      int,
       `SDB_IMPORTED`                  char NOT NULL,
       `Originator`                    varchar (20)
);
CREATE TABLE `DPC`
 (
       `DPC_NUMBE`                     varchar (16) NOT NULL,
       `DPC_NAME`                      varchar (16),
       `DPC_FLDN`                      int NOT NULL,
       `DPC_COMM`                      int,
       `DPC_MODE`                      varchar (2),
       `DPC_FORM`                      varchar (2),
       `DPC_TEXT`                      varchar (64),
       `SDB_IMPORTED`                  char NOT NULL,
       `Originator`                    varchar (20)
);
CREATE TABLE `DPF`
 (
       `DPF_NUMBE`                     varchar (16) NOT NULL,
       `DPF_TYPE`                      varchar (2) NOT NULL,
       `DPF_HEAD`                      varchar (64),
       `SDB_IMPORTED`                  char NOT NULL,
       `Originator`                    varchar (20)
);
CREATE TABLE `DST`
 (
       `DST_APID`                      int NOT NULL,
       `DST_ROUTE`                     varchar (60) NOT NULL,
       `SDB_IMPORTED`                  char NOT NULL,
       `Originator`                    varchar (20)
);
CREATE TABLE `GPC`
 (
       `GPC_NUMBE`                     varchar (16) NOT NULL,
       `GPC_POS`                       int NOT NULL,
       `GPC_WHERE`                     varchar (2) NOT NULL,
       `GPC_NAME`                      varchar (16) NOT NULL,
       `GPC_RAW`                       varchar (2),
       `GPC_MINIM`                     varchar (28) NOT NULL,
       `GPC_MAXIM`                     varchar (28) NOT NULL,
       `GPC_PRCLR`                     varchar (2) NOT NULL,
       `GPC_SYMBO`                     varchar (2),
       `GPC_LINE`                      varchar (2),
       `GPC_DOMAIN`                    varchar (10),
       `SDB_IMPORTED`                  char NOT NULL,
       `Originator`                    varchar (20)
);
CREATE TABLE `GPF`
 (
       `GPF_NUMBE`                     varchar (16) NOT NULL,
       `GPF_TYPE`                      varchar (2) NOT NULL,
       `GPF_HEAD`                      varchar (64),
       `GPF_SCROL`                     varchar (2),
       `GPF_HCOPY`                     varchar (2),
       `GPF_DAYS`                      int NOT NULL,
       `GPF_HOURS`                     int NOT NULL,
       `GPF_MINUT`                     int NOT NULL,
       `GPF_AXCLR`                     varchar (2) NOT NULL,
       `GPF_XTICK`                     int NOT NULL,
       `GPF_YTICK`                     int NOT NULL,
       `GPF_XGRID`                     int NOT NULL,
       `GPF_YGRID`                     int NOT NULL,
       `GPF_UPUN`                      int,
       `SDB_IMPORTED`                  char NOT NULL,
       `Originator`                    varchar (20)
);
CREATE TABLE `GRP`
 (
       `GRP_NAME`                      varchar (28) NOT NULL,
       `GRP_DESCR`                     varchar (48) NOT NULL,
       `GRP_GTYPE`                     varchar (4) NOT NULL
);
CREATE TABLE `GRPA`
 (
       `GRPA_GNAME`                    varchar (28) NOT NULL,
       `GRPA_PANAME`                   varchar (16) NOT NULL
);
CREATE TABLE `GRPK`
 (
       `GRPK_GNAME`                    varchar (28) NOT NULL,
       `GRPK_PKSPID`                   int NOT NULL
);
CREATE TABLE `LGF`
 (
       `LGF_IDENT`                     varchar (20) NOT NULL,
       `LGF_DESCR`                     varchar (64) NOT NULL,
       `LGF_POL1`                      varchar (28) NOT NULL,
       `LGF_POL2`                      varchar (28),
       `LGF_POL3`                      varchar (28),
       `LGF_POL4`                      varchar (28),
       `LGF_POL5`                      varchar (28)
);
CREATE TABLE `MCF`
 (
       `MCF_IDENT`                     varchar (20) NOT NULL,
       `MCF_DESCR`                     varchar (64) NOT NULL,
       `MCF_POL1`                      varchar (28) NOT NULL,
       `MCF_POL2`                      varchar (28),
       `MCF_POL3`                      varchar (28),
       `MCF_POL4`                      varchar (28),
       `MCF_POL5`                      varchar (28),
       `SDB_IMPORTED`                  char NOT NULL,
       `Originator`                    varchar (20)
);
CREATE TABLE `Name AutoCorrect Save Failures`
 (
       `Object Name`                   varchar (510),
       `Object Type`                   varchar (510),
       `Failure Reason`                        varchar (510),
       `Time`                  datetime
);
CREATE TABLE `OCP`
 (
       `OCP_NAME`                      varchar (16) NOT NULL,
       `OCP_POS`                       int NOT NULL,
       `OCP_TYPE`                      varchar (2) NOT NULL,
       `OCP_LVALU`                     varchar (28),
       `OCP_HVALU`                     varchar (28),
       `OCP_RLCHK`                     varchar (16),
       `OCP_VALPAR`                    int,
       `SDB_IMPORTED`                  char NOT NULL,
       `Originator`                    varchar (20)
);
CREATE TABLE `PAF`
 (
       `PAF_NUMBR`                     varchar (20) NOT NULL,
       `PAF_DESCR`                     varchar (48) NOT NULL,
       `PAF_RAWFMT`                    varchar (2),
       `PAF_NALIAS`                    int,
       `SDB_IMPORTED`                  char NOT NULL,
       `Originator`                    varchar (20)
);
CREATE TABLE `PAS`
 (
       `PAS_NUMBR`                     varchar (20) NOT NULL,
       `PAS_ALTXT`                     varchar (32) NOT NULL,
       `PAS_ALVAL`                     varchar (34) NOT NULL,
       `SDB_IMPORTED`                  char NOT NULL,
       `Originator`                    varchar (20)
);
CREATE TABLE `PCDF`
 (
       `PCDF_TCNAME`                   varchar (16) NOT NULL,
       `PCDF_DESC`                     varchar (48),
       `PCDF_TYPE`                     varchar (2) NOT NULL,
       `PCDF_LEN`                      int NOT NULL,
       `PCDF_BIT`                      int NOT NULL,
       `PCDF_PNAME`                    varchar (16),
       `PCDF_VALUE`                    varchar (20) NOT NULL,
       `PCDF_RADIX`                    varchar (2),
       `SDB_IMPORTED`                  char NOT NULL,
       `Originator`                    varchar (20)
);
CREATE TABLE `PCF`
 (
       `PCF_NAME`                      varchar (16) NOT NULL,
       `PCF_DESCR`                     varchar (48) NOT NULL,
       `PCF_PID`                       float,
       `PCF_UNIT`                      varchar (8),
       `PCF_PTC`                       int NOT NULL,
       `PCF_PFC`                       int NOT NULL,
       `PCF_WIDTH`                     int,
       `PCF_VALID`                     varchar (16),
       `PCF_RELATED`                   varchar (16),
       `PCF_CATEG`                     varchar (2) NOT NULL,
       `PCF_NATUR`                     varchar (2) NOT NULL,
       `PCF_CURTX`                     varchar (20),
       `PCF_INTER`                     varchar (2),
       `PCF_USCON`                     varchar (2),
       `PCF_DECIM`                     int,
       `PCF_PARVAL`                    varchar (28),
       `PCF_SUBSYS`                    varchar (16),
       `PCF_VALPAR`                    int,
       `PCF_SPTYPE`                    varchar (2),
       `SDB_IMPORTED`                  char NOT NULL,
       `Originator`                    varchar (20)
);
CREATE TABLE `PCPC`
 (
       `PCPC_PNAME`                    varchar (16) NOT NULL,
       `PCPC_DESC`                     varchar (48) NOT NULL,
       `PCPC_CODE`                     varchar (2),
       `SDB_IMPORTED`                  char NOT NULL,
       `Originator`                    varchar (20)
);
CREATE TABLE `PDI`
 (
       `PDI_GLOBAL`                    varchar (16) NOT NULL,
       `PDI_DETAIL`                    varchar (16) NOT NULL,
       `PDI_OFFSET`                    int NOT NULL
);
CREATE TABLE `PIC`
 (
       `PIC_TYPE`                      int NOT NULL,
       `PIC_STYPE`                     int NOT NULL,
       `PIC_PI1_OFF`                   int NOT NULL,
       `PIC_PI1_WID`                   int NOT NULL,
       `PIC_PI2_OFF`                   int NOT NULL,
       `PIC_PI2_WID`                   int NOT NULL,
       `SDB_IMPORTED`                  char NOT NULL,
       `Originator`                    varchar (20)
);
CREATE TABLE `PLF`
 (
       `PLF_NAME`                      varchar (16) NOT NULL,
       `PLF_SPID`                      int NOT NULL,
       `PLF_OFFBY`                     int NOT NULL,
       `PLF_OFFBI`                     int NOT NULL,
       `PLF_NBOCC`                     int,
       `PLF_LGOCC`                     int,
       `PLF_TIME`                      int,
       `PLF_TDOCC`                     int,
       `SDB_IMPORTED`                  char NOT NULL,
       `Originator`                    varchar (20)
);
CREATE TABLE `PPC`
 (
       `PPC_NUMBE`                     varchar (8) NOT NULL,
       `PPC_POS`                       int NOT NULL,
       `PPC_NAME`                      varchar (16) NOT NULL,
       `PPC_FORM`                      varchar (2),
       `SDB_IMPORTED`                  char NOT NULL,
       `Originator`                    varchar (20)
);
CREATE TABLE `PPF`
 (
       `PPF_NUMBE`                     varchar (8) NOT NULL,
       `PPF_HEAD`                      varchar (64),
       `PPF_NBPRO`                     int,
       `SDB_IMPORTED`                  char NOT NULL,
       `Originator`                    varchar (20)
);
CREATE TABLE `PRF`
 (
       `PRF_NUMBR`                     varchar (20) NOT NULL,
       `PRF_DESCR`                     varchar (48) NOT NULL,
       `PRF_INTER`                     varchar (2),
       `PRF_DSPFMT`                    varchar (2),
       `PRF_RADIX`                     varchar (2),
       `PRF_NRANGE`                    int,
       `PRF_UNIT`                      varchar (8),
       `SDB_IMPORTED`                  char NOT NULL,
       `Originator`                    varchar (20)
);
CREATE TABLE `PRV`
 (
       `PRV_NUMBR`                     varchar (20) NOT NULL,
       `PRV_MINVAL`                    varchar (510) NOT NULL,
       `PRV_MAXVAL`                    varchar (510),
       `SDB_IMPORTED`                  char NOT NULL,
       `Originator`                    varchar (20)
);
CREATE TABLE `PST`
 (
       `PST_NAME`                      varchar (16) NOT NULL,
       `PST_DESCR`                     varchar (48),
       `SDB_IMPORTED`                  char NOT NULL,
       `Originator`                    varchar (20)
);
CREATE TABLE `PSV`
 (
       `PSV_NAME`                      varchar (16) NOT NULL,
       `PSV_PVSID`                     varchar (16) NOT NULL,
       `PSV_DESCR`                     varchar (48),
       `SDB_IMPORTED`                  char NOT NULL,
       `Originator`                    varchar (20)
);
CREATE TABLE `PTV`
 (
       `PTV_CNAME`                     varchar (16) NOT NULL,
       `PTV_PARNAM`                    varchar (16) NOT NULL,
       `PTV_INTER`                     varchar (2),
       `PTV_VAL`                       varchar (34),
       `SDB_IMPORTED`                  char NOT NULL,
       `Originator`                    varchar (20)
);
CREATE TABLE `PVS`
 (
       `PVS_ID`                        varchar (16) NOT NULL,
       `PVS_PSID`                      varchar (16) NOT NULL,
       `PVS_PNAME`                     varchar (16) NOT NULL,
       `PVS_INTER`                     varchar (2),
       `PVS_VALS`                      varchar (34),
       `PVS_BIT`                       int NOT NULL,
       `SDB_IMPORTED`                  char NOT NULL,
       `Originator`                    varchar (20)
);
CREATE TABLE `SPC`
 (
       `SPC_NUMBE`                     varchar (16) NOT NULL,
       `SPC_POS`                       int NOT NULL,
       `SPC_NAME`                      varchar (16) NOT NULL,
       `SPC_UPDT`                      varchar (2),
       `SPC_MODE`                      varchar (2),
       `SPC_FORM`                      varchar (2),
       `SPC_BACK`                      varchar (2),
       `SPC_FORE`                      varchar (2) NOT NULL,
       `SDB_IMPORTED`                  char NOT NULL,
       `Originator`                    varchar (20)
);
CREATE TABLE `SPE`
 (
       `SPE_PNAME`                     varchar (16) NOT NULL,
       `SPE_OLEXPR`                    text (255),
       `SPE_Description`                       text (255),
       `SDB_IMPORTED`                  char NOT NULL,
       `Originator`                    varchar (20)
);
CREATE TABLE `SPF`
 (
       `SPF_NUMBE`                     varchar (16) NOT NULL,
       `SPF_HEAD`                      varchar (64),
       `SPF_NPAR`                      int NOT NULL,
       `SPF_UPUN`                      int NOT NULL,
       `SDB_IMPORTED`                  char NOT NULL,
       `Originator`                    varchar (20)
);
CREATE TABLE `SSY`
 (
       `SSY_ID`                        int,
       `SSY_NAME`                      varchar (16),
       `Originator`                    varchar (20),
       `SDB_IMPORTED`                  char NOT NULL
);
CREATE TABLE `tblCAFselectCAFengfmt`
 (
       `TYPE`                  varchar (2),
       `Description`                   varchar (100)
);
CREATE TABLE `tblCAFselectCAFinter`
 (
       `Letter`                        varchar (2),
       `Description`                   varchar (50)
);
CREATE TABLE `tblCAFselectCAFradix`
 (
       `RADIX`                 varchar (2),
       `Description`                   varchar (50)
);
CREATE TABLE `tblCAFselectCAFrawfmt`
 (
       `TYPE`                  varchar (2),
       `Description`                   varchar (100)
);
CREATE TABLE `tblCategoryRules`
 (
       `Category`                      varchar (510),
       `Service`                       int,
       `Sub_Service`                   int
);
CREATE TABLE `tblCCFbufferCCF`
 (
       `CCF_CNAME`                     varchar (16) NOT NULL,
       `CCF_DESCR`                     varchar (48) NOT NULL,
       `CCF_DESCR2`                    varchar (128),
       `CCF_CTYPE`                     varchar (16),
       `CCF_CRITICAL`                  varchar (2),
       `CCF_PKTID`                     varchar (16) NOT NULL,
       `CCF_TYPE`                      int,
       `CCF_STYPE`                     int,
       `CCF_APID`                      int,
       `CCF_NPARS`                     int,
       `CCF_PLAN`                      varchar (2),
       `CCF_EXEC`                      varchar (2),
       `CCF_ILSCOPE`                   varchar (2),
       `CCF_ILSTAGE`                   varchar (2),
       `CCF_SUBSYS`                    int,
       `CCF_HIPRI`                     varchar (2),
       `CCF_MAPID`                     int,
       `CCF_DEFSET`                    varchar (16),
       `CCF_RAPID`                     int,
       `CCF_ACK`                       int,
       `CCF_SUBSCHEDID`                        int
);
CREATE TABLE `tblCCFbufferCDF`
 (
       `CDF_CNAME`                     varchar (16),
       `CDF_ELTYPE`                    varchar (2),
       `CDF_DESCR`                     varchar (48),
       `CDF_ELLEN`                     int,
       `CDF_BIT`                       int,
       `CDF_GRPSIZE`                   int,
       `CDF_PNAME`                     varchar (16),
       `CDF_INTER`                     varchar (2),
       `CDF_VALUE`                     varchar (34),
       `CDF_TMID`                      varchar (16)
);
CREATE TABLE `tblCCFbufferCVP`
 (
       `CVP_TASK`                      varchar (16) NOT NULL,
       `CVP_TYPE`                      varchar (2) NOT NULL,
       `CVP_CVSID`                     int NOT NULL
);
CREATE TABLE `tblCCFbufferPTV`
 (
       `PTV_CNAME`                     varchar (16),
       `PTV_PARNAM`                    varchar (16),
       `PTV_INTER`                     varchar (2),
       `PTV_VAL`                       varchar (34)
);
CREATE TABLE `tblCCFselectCCFctype`
 (
       `Type`                  varchar (2),
       `Description`                   varchar (100)
);
CREATE TABLE `tblCCFselectCCFilscope`
 (
       `Type`                  varchar (2),
       `Description`                   varchar (100)
);
CREATE TABLE `tblCCFselectCCFilstage`
 (
       `Letter`                        varchar (2),
       `Description`                   varchar (100)
);
CREATE TABLE `tblCCFselectCCFsubsys`
 (
       `Number`                        int,
       `subsystem`                     varchar (20)
);
CREATE TABLE `tblCCFselectCCFyesNo`
 (
       `Type`                  varchar (2),
       `Description`                   varchar (10)
);
CREATE TABLE `tblCCFselectCDFeltype`
 (
       `ELTYPE`                        varchar (2),
       `Description`                   varchar (112)
);
CREATE TABLE `tblCCFselectCDFinter`
 (
       `INTER`                 varchar (2),
       `DESCRIPTION`                   varchar (210)
);
CREATE TABLE `tblCheckConfig`
 (
       `Id`                    int,
       `Field`                 varchar (50),
       `Check Type`                    varchar (50),
       `Category`                      varchar (20) NOT NULL,
       `IsGeneric`                     char NOT NULL,
       `Enabled`                       char NOT NULL,
       `Order`                 int,
       `p1`                    varchar (100),
       `p2`                    varchar (100),
       `p3`                    varchar (100),
       `p4`                    varchar (100),
       `p5`                    varchar (100)
);
CREATE TABLE `tblCheckDescription`
 (
       `Id`                    int,
       `Field`                 varchar (50),
       `Check Type`                    varchar (50),
       `Category`                      varchar (20) NOT NULL,
       `IsGeneric`                     char NOT NULL,
       `Enabled`                       char NOT NULL,
       `Description`                   varchar (510),
       `Rationale`                     varchar (510),
       `BEPI use`                      char NOT NULL,
       `SOLO_use`                      char NOT NULL
);
CREATE TABLE `tblCheckErrors`
 (
       `Error_No`                      int,
       `Table_Name`                    varchar (100),
       `Check_Type`                    varchar (100),
       `Category`                      varchar (20),
       `Column_Name`                   varchar (100),
       `Description`                   text (255),
       `Justification`                 varchar (510)
);
CREATE TABLE `tblCheckPLFRangeConfig`
 (
       `SPID`                  int
);
CREATE TABLE `tblCheckSupportSDFCompletenessCDF`
 (
       `CSS_SQNAME`                    varchar (16),
       `CSS_ENTRY`                     int,
       `CSS_ELEMID`                    varchar (16),
       `CDF_BIT`                       int,
       `CDF_PNAME`                     varchar (16)
);
CREATE TABLE `tblCheckSupportSDFCompletenessCSP`
 (
       `CSS_SQNAME`                    varchar (16),
       `CSS_ENTRY`                     int,
       `CSS_ELEMID`                    varchar (16),
       `CSP_FPNUM`                     int,
       `CSP_FPNAME`                    varchar (16)
);
CREATE TABLE `tblConfigKeys`
 (
       `Sequence`                      int,
       `Column`                        varchar (40),
       `Key`                   char NOT NULL
);
CREATE TABLE `tblConfigS2KParameterTypes`
 (
       `PTC`                   int NOT NULL,
       `PFC_LB`                        int NOT NULL,
       `PFC_UB`                        int,
       `S2K_TYPE`                      varchar (2),
       `S2K_TYPE_Description`                  varchar (150),
       `LENGTH`                        varchar (100),
       `SUPPORTED`                     char NOT NULL,
       `S2K_GENERIC`                   char NOT NULL,
       `Description`                   varchar (200)
);
CREATE TABLE `tblConfigSCOSTables`
 (
       `Table`                 varchar (100) NOT NULL,
       `Subset`                        varchar (100) NOT NULL,
       `ImportSeq`                     int,
       `Exportable`                    char NOT NULL
);
CREATE TABLE `tblConfigStdCalPAF`
 (
       `PAF_NUMBR`                     varchar (20) NOT NULL,
       `PAF_DESCR`                     varchar (48) NOT NULL,
       `PAF_RAWFMT`                    varchar (2),
       `PAF_NALIAS`                    int,
       `SDB_IMPORTED`                  char NOT NULL,
       `Originator`                    varchar (20)
);
CREATE TABLE `tblConfigStdCalPAS`
 (
       `PAS_NUMBR`                     varchar (20) NOT NULL,
       `PAS_ALTXT`                     varchar (32) NOT NULL,
       `PAS_ALVAL`                     varchar (34) NOT NULL,
       `SDB_IMPORTED`                  char NOT NULL,
       `Originator`                    varchar (20)
);
CREATE TABLE `tblConfigStdCalTXF`
 (
       `TXF_NUMBR`                     varchar (20) NOT NULL,
       `TXF_DESCR`                     varchar (64) NOT NULL,
       `TXF_RAWFMT`                    varchar (2) NOT NULL,
       `TXF_NALIAS`                    int,
       `SDB_IMPORTED`                  char NOT NULL,
       `Originator`                    varchar (20)
);
CREATE TABLE `tblConfigStdCalTXP`
 (
       `TXP_NUMBR`                     varchar (20) NOT NULL,
       `TXP_FROM`                      varchar (28) NOT NULL,
       `TXP_TO`                        varchar (28) NOT NULL,
       `TXP_ALTXT`                     varchar (28) NOT NULL,
       `SDB_IMPORTED`                  char NOT NULL,
       `Originator`                    varchar (20)
);
CREATE TABLE `tblConfiguration`
 (
       `Name`                  varchar (100),
       `Value`                 varchar (100),
       `Order`                 int
);
CREATE TABLE `tblCPCbufferCPC`
 (
       `CPC_PNAME`                     varchar (16),
       `CPC_DESCR`                     varchar (48),
       `CPC_PTC`                       int,
       `CPC_PFC`                       int,
       `CPC_DISPFMT`                   varchar (2),
       `CPC_RADIX`                     varchar (2),
       `CPC_UNIT`                      varchar (8),
       `CPC_CATEG`                     varchar (2),
       `CPC_PRFREF`                    int,
       `CPC_CCAREF`                    int,
       `CPC_PAFREF`                    int,
       `CPC_INTER`                     varchar (2),
       `CPC_DEFVAL`                    varchar (34)
);
CREATE TABLE `tblCPCselectCPCcateg`
 (
       `Letter`                        varchar (2),
       `description`                   varchar (100)
);
CREATE TABLE `tblCPCselectCPCdispfmt`
 (
       `Rep`                   varchar (2),
       `desc`                  varchar (100)
);
CREATE TABLE `tblCPCselectCPCinter`
 (
       `Letter`                        varchar (2),
       `Description`                   varchar (100)
);
CREATE TABLE `tblCVSselectCVEcheck`
 (
       `Letter`                        varchar (2),
       `Description`                   varchar (200)
);
CREATE TABLE `tblCVSselectCVEinter`
 (
       `Letter`                        varchar (2),
       `Description`                   varchar (150)
);
CREATE TABLE `tblCVSselectCVSsource`
 (
       `Value`                 varchar (2),
       `text`                  varchar (250)
);
CREATE TABLE `tblCVSselectCVStype`
 (
       `Stage Type`                    varchar (2),
       `Description`                   varchar (120)
);
CREATE TABLE `tblDBMtables`
 (
       `Rank`                  int,
       `Tablename`                     varchar (100),
       `Filename`                      varchar (100),
       `MIB_Import`                    char NOT NULL,
       `key1`                  varchar (100),
       `key2`                  varchar (100),
       `Selected_Export`                       char NOT NULL
);
CREATE TABLE `tblExportLog`
 (
       `Filename`                      varchar (510),
       `Exported`                      varchar (100),
       `Date`                  datetime
);
CREATE TABLE `tblImportLog`
 (
       `Filename`                      varchar (510),
       `Imported`                      varchar (100),
       `Date`                  datetime
);
CREATE TABLE `tblLogChangeLogTemp`
 (
       `TableName`                     varchar (24),
       `Reference`                     varchar (100),
       `FieldName`                     varchar (40),
       `ChangedFrom`                   varchar (100),
       `ChangedTo`                     varchar (100),
       `ChangeDate`                    date,
       `ChangeTime`                    datetime,
       `Action`                        varchar (100)
);
CREATE TABLE `tblOCFselectOCFcodin`
 (
       `CODIN`                 varchar (2),
       `Description`                   varchar (150)
);
CREATE TABLE `tblOCFselectOCFinter`
 (
       `INTER`                 varchar (2),
       `Description`                   varchar (100)
);
CREATE TABLE `tblOCFselectOCPtype`
 (
       `TypeLetter`                    varchar (2),
       `TypeDiscription`                       varchar (100)
);
CREATE TABLE `tblPacketFilingConfig`
 (
       `SPID`                  int,
       `RecType`                       varchar (20),
       `Stream`                        varchar (100),
       `Directory`                     varchar (160),
       `SecondKey`                     int
);
CREATE TABLE `tblPayloadChecks`
 (
       `Payload`                       varchar (510),
       `Name`                  varchar (510),
       `SUBSYS`                        int,
       `SPIDlow`                       int,
       `SPIDhigh`                      int
);
CREATE TABLE `tblPCFbufferOCF`
 (
       `OCF_NAME`                      varchar (16),
       `OCF_NBCHCK`                    int NOT NULL,
       `OCF_NBOOL`                     int,
       `OCF_INTER`                     varchar (2) NOT NULL,
       `OCF_CODIN`                     varchar (2)
);
CREATE TABLE `tblPCFbufferPCF`
 (
       `PCF_NAME`                      varchar (16) NOT NULL,
       `PCF_DESCR`                     varchar (48),
       `PCF_PID`                       int,
       `PCF_UNIT`                      varchar (8),
       `PCF_PTC`                       int NOT NULL,
       `PCF_PFC`                       int NOT NULL,
       `PCF_WIDTH`                     int,
       `PCF_VALID`                     varchar (16),
       `PCF_RELATED`                   varchar (16),
       `PCF_CATEG`                     varchar (2) NOT NULL,
       `PCF_NATUR`                     varchar (2) NOT NULL,
       `PCF_CURTX`                     int,
       `PCF_INTER`                     varchar (2),
       `PCF_USCON`                     varchar (2),
       `PCF_DECIM`                     int,
       `PCF_PARVAL`                    varchar (28),
       `PCF_SUBSYS`                    varchar (16),
       `PCF_VALPAR`                    int,
       `PCF_SPTYPE`                    varchar (2)
);
CREATE TABLE `tblPCFbufferPLF`
 (
       `PLF_NAME`                      varchar (16) NOT NULL,
       `PLF_SPID`                      int NOT NULL,
       `PLF_OFFBY`                     int NOT NULL,
       `PLF_OFFBI`                     int NOT NULL,
       `PLF_NBOCC`                     int,
       `PLF_LGOCC`                     int,
       `PLF_TIME`                      int,
       `PLF_TDOCC`                     int
);
CREATE TABLE `tblPCFselectPCFcateg`
 (
       `Letter`                        varchar (2),
       `Description`                   varchar (100)
);
CREATE TABLE `tblPCFselectPCFinter`
 (
       `Letter`                        varchar (2),
       `Description`                   varchar (50)
);
CREATE TABLE `tblPCFselectPCFnatur`
 (
       `Letter`                        varchar (2),
       `Description`                   varchar (40)
);
CREATE TABLE `tblPCFselectPCFstype`
 (
       `Letter`                        varchar (2),
       `Description`                   varchar (50)
);
CREATE TABLE `tblPCFselectVPDchoice`
 (
       `CHOICE`                        varchar (2),
       `Description`                   varchar (130)
);
CREATE TABLE `tblPCFselectVPDdchar`
 (
       `DCHAR`                 varchar (2),
       `Description`                   varchar (130)
);
CREATE TABLE `tblPCFselectVPDform`
 (
       `FORM`                  varchar (2),
       `Description`                   varchar (100)
);
CREATE TABLE `tblPCFselectVPDjustify`
 (
       `JUSTIFY`                       varchar (2),
       `Description`                   varchar (130)
);
CREATE TABLE `tblPCFselectVPDnewline`
 (
       `NEWLINE`                       varchar (2),
       `Description`                   varchar (130)
);
CREATE TABLE `tblPCFselectVPDpidref`
 (
       `PIDREF`                        varchar (2),
       `Description`                   varchar (130)
);
CREATE TABLE `tblPICrule`
 (
       `PIC_TYPE`                      int,
       `PIC_STYPE`                     int,
       `PIC_PI1_OFF`                   int,
       `PIC_PI1_WID`                   int
);
CREATE TABLE `tblSVNimportLog`
 (
       `Name`                  varchar (100),
       `Status`                        varchar (100),
       `Date`                  varchar (100)
);
CREATE TABLE `tblTableDef`
 (
       `Fieldname`                     varchar (40)
);
CREATE TABLE `tblTCPselectPCDFradix`
 (
       `RADIX`                 varchar (2),
       `Description`                   varchar (22)
);
CREATE TABLE `tblTCPselectPCDFtype`
 (
       `TYPE`                  varchar (2),
       `Description`                   varchar (150)
);
CREATE TABLE `tblTCPselectPCPCcode`
 (
       `RADIX`                 varchar (2),
       `Description`                   varchar (40)
);
CREATE TABLE `tblTempVPD_OFFBI`
 (
       `VPD_SPID`                      int,
       `VPD_OFFBI`                     int,
       `PCF_NAME`                      varchar (100),
       `PCF_LENGTH`                    int
);
CREATE TABLE `tblTPCFselectPIDcheck`
 (
       `CHECK`                 int,
       `Description`                   varchar (40)
);
CREATE TABLE `tblTPCFselectPIDevent`
 (
       `EVENT`                 varchar (2),
       `Description`                   varchar (100)
);
CREATE TABLE `tblTPCFselectPIDtime`
 (
       `TIME`                  varchar (2),
       `Description`                   varchar (50)
);
CREATE TABLE `tblTPCFselectPIDvalid`
 (
       `VALID`                 varchar (2),
       `Description`                   varchar (40)
);
CREATE TABLE `tblTXFselectTXFrawfmt`
 (
       `RAWFMT`                        varchar (2),
       `Description`                   varchar (50)
);
CREATE TABLE `TCP`
 (
       `TCP_ID`                        varchar (16) NOT NULL,
       `TCP_DESC`                      varchar (48) NOT NULL,
       `SDB_IMPORTED`                  char NOT NULL,
       `Originator`                    varchar (20)
);
CREATE TABLE `TPCF`
 (
       `TPCF_SPID`                     int NOT NULL,
       `TPCF_NAME`                     varchar (16) NOT NULL,
       `TPCF_SIZE`                     int,
       `SDB_IMPORTED`                  char NOT NULL,
       `Originator`                    varchar (20)
);
CREATE TABLE `TXF`
 (
       `TXF_NUMBR`                     varchar (20) NOT NULL,
       `TXF_DESCR`                     varchar (64) NOT NULL,
       `TXF_RAWFMT`                    varchar (2) NOT NULL,
       `TXF_NALIAS`                    int,
       `SDB_IMPORTED`                  char NOT NULL,
       `Originator`                    varchar (20)
);
CREATE TABLE `TXP`
 (
       `TXP_NUMBR`                     varchar (20) NOT NULL,
       `TXP_FROM`                      varchar (28) NOT NULL,
       `TXP_TO`                        varchar (28) NOT NULL,
       `TXP_ALTXT`                     varchar (28) NOT NULL,
       `SDB_IMPORTED`                  char NOT NULL,
       `Originator`                    varchar (20)
);
CREATE TABLE `VDF`
 (
       `VDF_NAME`                      varchar (64) NOT NULL,
       `VDF_COMMENT`                   varchar (64) NOT NULL,
       `VDF_DOMAINID`                  int,
       `VDF_RELEASE`                   varchar (32) NOT NULL,
       `VDF_ISSUE`                     varchar (32) NOT NULL,
       `Originator`                    varchar (20)
);
CREATE TABLE `CVP`
 (
       `CVP_TASK`                      varchar (16) NOT NULL,
       `CVP_TYPE`                      varchar (2) NOT NULL,
       `CVP_CVSID`                     int NOT NULL,
       `SDB_IMPORTED`                  char NOT NULL,
       `Originator`                    varchar (20)
);
CREATE TABLE `OCF`
 (
       `OCF_NAME`                      varchar (16) NOT NULL,
       `OCF_NBCHCK`                    int NOT NULL,
       `OCF_NBOOL`                     int NOT NULL,
       `OCF_INTER`                     varchar (2) NOT NULL,
       `OCF_CODIN`                     varchar (2) NOT NULL,
       `SDB_IMPORTED`                  char NOT NULL,
       `Originator`                    varchar (20)
);
CREATE TABLE `Paste Errors`
 (
       `PIC_TYPE`                      text (255),
       `PIC_STYPE`                     text (255),
       `PIC_PI1_OFF`                   text (255),
       `PIC_PI1_WID`                   text (255),
       `PIC_PI2_OFF`                   text (255),
       `PIC_PI2_WID`                   text (255)
);
CREATE TABLE `PID`
 (
       `PID_TYPE`                      int NOT NULL,
       `PID_STYPE`                     int NOT NULL,
       `PID_APID`                      int NOT NULL,
       `PID_PI1_VAL`                   int NOT NULL,
       `PID_PI2_VAL`                   int NOT NULL,
       `PID_SPID`                      int NOT NULL,
       `PID_DESCR`                     varchar (128) NOT NULL,
       `PID_UNIT`                      varchar (16),
       `PID_TPSD`                      int,
       `PID_DFHSIZE`                   int NOT NULL,
       `PID_TIME`                      varchar (2),
       `PID_INTER`                     int,
       `PID_VALID`                     varchar (2),
       `PID_CHECK`                     int,
       `PID_EVENT`                     varchar (2),
       `PID_EVID`                      varchar (34),
       `SDB_IMPORTED`                  char NOT NULL,
       `Originator`                    varchar (20)
);
CREATE TABLE `PSM`
 (
       `PSM_NAME`                      varchar (16) NOT NULL,
       `PSM_TYPE`                      varchar (2) NOT NULL,
       `PSM_PARSET`                    varchar (16) NOT NULL,
       `SDB_IMPORTED`                  char NOT NULL,
       `Originator`                    varchar (20)
);
CREATE TABLE `SDF`
 (
       `SDF_SQNAME`                    varchar (16) NOT NULL,
       `SDF_ENTRY`                     int NOT NULL,
       `SDF_ELEMID`                    varchar (16) NOT NULL,
       `SDF_POS`                       int NOT NULL,
       `SDF_PNAME`                     varchar (16),
       `SDF_FTYPE`                     varchar (2),
       `SDF_VTYPE`                     varchar (2) NOT NULL,
       `SDF_VALUE`                     varchar (34),
       `SDF_VALSET`                    varchar (16),
       `SDB_IMPORTED`                  char NOT NULL,
       `Originator`                    varchar (20)
);
CREATE TABLE `SW_PARA`
 (
       `SCOS_NAME`                     varchar (16) NOT NULL,
       `SW_NAME`                       varchar (64),
       `SW_DESCR`                      varchar (510)
);
CREATE TABLE `tblCCAselectCCAtype`
 (
       `Letter`                        varchar (2),
       `Description`                   varchar (100)
);
CREATE TABLE `tblCCFselectCCFplan`
 (
       `Plannable`                     varchar (2),
       `Description`                   varchar (100)
);
CREATE TABLE `tblChangeLog`
 (
       `TableName`                     varchar (24),
       `Reference`                     varchar (100),
       `FieldName`                     varchar (40),
       `ChangedFrom`                   text (255),
       `ChangedTo`                     text (255),
       `ChangeDate`                    date,
       `ChangeTime`                    datetime,
       `Action`                        varchar (100)
);
CREATE TABLE `tblConfig`
 (
       `Project`                       varchar (100),
       `MIB_Path`                      varchar (510),
       `MIB_Export_Path`                       varchar (510),
       `CAF_INTER`                     char NOT NULL,
       `Tool_Version`                  varchar (100),
       `PUSICD`                        varchar (100),
       `MIBICD`                        varchar (100),
       `ExportErrorFile`                       varchar (510),
       `PL`                    varchar (510)
);
CREATE TABLE `tblConfigStdCalPCF`
 (
       `PCF_NAME`                      varchar (16) NOT NULL,
       `PCF_DESCR`                     varchar (48) NOT NULL,
       `PCF_PID`                       float,
       `PCF_UNIT`                      varchar (8),
       `PCF_PTC`                       int NOT NULL,
       `PCF_PFC`                       int NOT NULL,
       `PCF_WIDTH`                     int,
       `PCF_VALID`                     varchar (16),
       `PCF_RELATED`                   varchar (16),
       `PCF_CATEG`                     varchar (2) NOT NULL,
       `PCF_NATUR`                     varchar (2) NOT NULL,
       `PCF_CURTX`                     varchar (20),
       `PCF_INTER`                     varchar (2),
       `PCF_USCON`                     varchar (2),
       `PCF_DECIM`                     int,
       `PCF_PARVAL`                    varchar (28),
       `PCF_SUBSYS`                    varchar (16),
       `PCF_VALPAR`                    int,
       `PCF_SPTYPE`                    varchar (2),
       `SDB_IMPORTED`                  char NOT NULL,
       `Originator`                    varchar (20)
);
CREATE TABLE `tblCPCselectCPCradix`
 (
       `RADIX`                 varchar (2),
       `Description`                   varchar (22)
);
CREATE TABLE `tblLogChangeLogDef`
 (
       `Fieldname`                     varchar (40)
);
CREATE TABLE `tblPCFbufferOCP`
 (
       `OCP_NAME`                      varchar (16),
       `OCP_POS`                       int,
       `OCP_TYPE`                      varchar (2) NOT NULL,
       `OCP_LVALU`                     varchar (20),
       `OCP_HVALU`                     varchar (20),
       `OCP_RLCHK`                     varchar (12),
       `OCP_VALPAR`                    int
);
CREATE TABLE `tblPCFselectPCFuscon`
 (
       `Letter`                        varchar (2),
       `Description`                   varchar (100)
);
CREATE TABLE `tblSVNexportLog`
 (
       `Name`                  varchar (100),
       `Status`                        varchar (100),
       `Date`                  varchar (100)
);
CREATE TABLE `VPD`
 (
       `VPD_TPSD`                      int NOT NULL,
       `VPD_POS`                       int NOT NULL,
       `VPD_NAME`                      varchar (16) NOT NULL,
       `VPD_GRPSIZE`                   int,
       `VPD_FIXREP`                    int,
       `VPD_CHOICE`                    varchar (2),
       `VPD_PIDREF`                    varchar (2),
       `VPD_DISDESC`                   varchar (48),
       `VPD_WIDTH`                     int NOT NULL,
       `VPD_JUSTIFY`                   varchar (2),
       `VPD_NEWLINE`                   varchar (2),
       `VPD_DCHAR`                     int,
       `VPD_FORM`                      varchar (2),
       `VPD_OFFSET`                    int,
       `SDB_IMPORTED`                  char NOT NULL,
       `Originator`                    varchar (20)
);
CREATE TABLE IDB(
creation_datetime  datetime,
version  varchar(64) not null
);
