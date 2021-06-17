#librerias
library(odbc)
library(data.table)
library(DBI)
library(dplyr)
library(lubridate)
library(stringr)
library(readxl)
library(RODBC)

# CONEXIONES A BASE DE DATOS
con44<-dbConnect(odbc::odbc(), "con_prod", UID="sa", PWD= "grup03mpresari@lsrs", enconding='UTF-8')

###################################################################################################################################################################
BSID<-as.data.table(dbGetQuery(con44, "SELECT [BUKRS],[KUNNR],[UMSKS],[AUGDT],[AUGBL],
[BUDAT],[BELNR],[XBLNR],[BLART],[SHKZG],[DMBTR],[SGTXT],[ZTERM],[ZBD1T],[VBELN],[HKONT]
FROM [CHP].[chp].[BSID]
WHERE BUKRS='1000'"))

names(BSID)<-c("Sociedad","Cod_Cliente","Clase_Operacion","Fecha","Cod_DocCompras","Fecha_Contabilizacion",
               "Num_Documento","Referencia","Clase_Documento","DebeHaber","ImporteML","Texto","Condicion_Pago",
               "Dias_Pago","Cod_Factura","CodCuentaContable")

BSID$Fecha_Contabilizacion=paste(substr(BSID$Fecha_Contabilizacion, start = 1, stop = 4),
                                 substr(BSID$Fecha_Contabilizacion, start = 5, stop = 6),
                                 substr(BSID$Fecha_Contabilizacion, start = 7, stop = 8), sep = "/")
BSID$Fecha_Contabilizacion=as.Date(BSID$Fecha_Contabilizacion,format="%Y/%m/%d")

BSID<-BSID[Cod_Cliente!="0000100080"&Cod_Cliente!="0002000165"&Cod_Cliente!="0000100107"&Cod_Cliente!="0001000057"&
             Cod_Cliente!="0000100035"&Cod_Cliente!="0000100060"&Cod_Cliente!="0001000049"&Cod_Cliente!="0001000071"&
             Cod_Cliente!="0001000260"&Cod_Cliente!="0001000105",]


BSID$Vence_Neto=BSID$Fecha_Contabilizacion+BSID$Dias_Pago
BSID$ImporteML = ifelse(BSID$DebeHaber=="H", (BSID$ImporteML)*-1, BSID$ImporteML)

BSID$mora=as.integer(Sys.Date()-BSID$Vence_Neto)
BSID$condicion=ifelse(BSID$mora>0 & BSID$mora<=30, '2. 1 a 30 Dias',
                      ifelse(BSID$mora>30 & BSID$mora<=60, '3. 31 a 60 Dias',
                             ifelse(BSID$mora>60 & BSID$mora<=90, '4. 61 a 90 Dias',
                                    ifelse(BSID$mora>90 & BSID$mora<=120, '5. 91 a 120 Dias',
                                           ifelse(BSID$mora>120 & BSID$mora<=180, '6. 121 a 180 Dias',
                                                  ifelse(BSID$mora>180 & BSID$mora<=365, '7. 181 a 365 Dias',
                                                         ifelse(BSID$mora>365, '8. 365 en adelante', '1.Por vencer')))))))

names(BSID)<-c("id_sociedad", "id_cliente", "Clase_Operacion", "Fecha", "Cod_DocCompras", "Fecha_Contabilizacion", "num_documento","Referencia",
               "Clase_Documento","DebeHaber","ImporteML","Texto","Condicion_Pago","Dias_Pago","id_Factura","CodCuentaContable","Vence_Neto",
               "mora","condicion")

################################################################################
con44<-dbConnect(odbc::odbc(), "con_prod", UID="sa", PWD= "grup03mpresari@lsrs", enconding='UTF-8')

VBRK<-as.data.table(dbGetQuery(con44, "SELECT [VBELN],[FKDAT],[BZIRK],[NETWR],[MWSBK]
                                  FROM [CHP].[chp].[VBRK]"))

names(VBRK)<-c("id_Factura","Fecha_Factura","id_zonaVenta","Valor_Neto","Impuesto")
VBRK$Mes_Factura=as.integer(substr(VBRK$Fecha_Factura, start = 5, stop = 6))
VBRK$Fecha_Factura=paste(substr(VBRK$Fecha_Factura, start = 1, stop = 4),
                         substr(VBRK$Fecha_Factura, start = 5, stop = 6),
                         substr(VBRK$Fecha_Factura, start = 7, stop = 8), sep = "/")
VBRK$Fecha_Factura=as.Date(VBRK$Fecha_Factura,format="%Y/%m/%d")
VBRK<-VBRK[,-"Impuesto"]

####################
con44<-dbConnect(odbc::odbc(), "con_prod", UID="sa", PWD= "grup03mpresari@lsrs", enconding='UTF-8')

VBPA<-as.data.table(dbGetQuery(con44, "SELECT [VBELN],[PARVW],[PERNR]
                                  FROM [CHP].[chp].[VBPA]
                               WHERE PARVW = 'VE'"))

names(VBPA)<-c("id_Factura","Tipo","Cod_Vendedor")

################################################################################
cartera<-left_join(BSID,VBRK,by="id_Factura")
cartera<-left_join(cartera,VBPA,by="id_Factura")
cartera$FechaCarga<-Sys.Date()
#dbCreateTable(conSRSCreacion, "FI_dim_cartera", cartera)

################################################################################
conSRSCreacion <- dbConnect(odbc::odbc(), "conexion_ASTS", UID="sa", PWD= "grup03mpresari@lsrs", enconding='UTF-8')
MaxFechaCarga<-dbGetQuery(conSRSCreacion, "SELECT max(FechaCarga)
                          FROM [ASTS].[dbo].[FI_dim_cartera]")
names(MaxFechaCarga)[1]<-"Fechaa"
MaxFechaCarga<-max(MaxFechaCarga$Fechaa)

if (MaxFechaCarga==Sys.Date()){
  query=paste0("DELETE FROM FI_dim_cartera where FechaCarga='", MaxFechaCarga,"'")
  dbSendQuery(conSRSCreacion, query)
  conSRS<-odbcConnect("conexion_ASTS",uid="sa",pwd="grup03mpresari@lsrs")
  sqlSave(conSRS, dat = cartera, tablename = "FI_dim_cartera", rownames = F, append=TRUE)
  print(paste("carga exitosa Cartera y C. mismo dia, fecha: ", now(), sep = ""))
  
}else{
  conSRS<-odbcConnect("conexion_ASTS",uid="sa",pwd="grup03mpresari@lsrs")
  sqlSave(conSRS, dat = cartera, tablename = "FI_dim_cartera", rownames = F, append=TRUE)
  print(paste("carga exitosa Cartera y C. nuevo dia, fecha: ", now(), sep = ""))
}

################################################################################################################################################################
################################################################################################################################################################
# clientesBase <- as.data.table(read_excel("D:/JorgeQ/R/Financiero/Cobranzas/REPORTE CARTERA 04.xlsx", 
#                                  sheet = "CLIENTES BASE"))
# 
# clientesBase<-clientesBase[,c("Cod SAP TXT","Tipo Cliente")]
# names(clientesBase)<-c("id_cliente","tipoCliente")
# 
# dbCreateTable(conSRSCreacion, "FI_dim_tipoDeClientes", clientesBase)
# sqlSave(conSRS, dat = clientesBase, tablename = "FI_dim_tipoDeClientes", rownames = F, append=TRUE)

################################################################################
con44<-dbConnect(odbc::odbc(), "con_prod", UID="sa", PWD= "grup03mpresari@lsrs", enconding='UTF-8')
SKAT<-as.data.table(dbGetQuery(con44, "SELECT [SAKNR],[TXT50]
FROM [CHP].[chp].[SKAT]
WHERE KTOPL = 'PUCC'"))

names(SKAT)<-c("CodCuentaContable","DescCuentaContable")
SKAT<-SKAT[!duplicated(SKAT$CodCuentaContable),]
#dbCreateTable(conSRSCreacion, "FI_dim_cuentaContable", SKAT)

##### clientes nuevos
DCCExistentes<-as.data.table(dbGetQuery(conSRSCreacion, "SELECT [CodCuentaContable] FROM [ASTS].[dbo].[FI_dim_cuentaContable]"))
DCCNuevos<-anti_join(SKAT, DCCExistentes, by="CodCuentaContable")
sqlSave(conSRS, dat = DCCNuevos, tablename = "FI_dim_cuentaContable", rownames = F, append=TRUE)

################################################################################
print(paste("proceso carga de datos Cartera Y Cobranza finalizada con exito, fecha: ", now(), sep = ""))

