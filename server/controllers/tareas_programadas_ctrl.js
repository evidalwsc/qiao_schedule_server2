const enviarEmail = require('../../handlers/email');
const cryptoJs = require('crypto-js');
const client = require('../config/db.client');
const clientExp = require('../config/db.client.experienciadigital');
const moment=require('moment');
const QRCode = require("qrcode");
const PDF = require('pdfkit');//Importando la libreria de PDFkit
const fs = require('fs');
const { verify } = require('crypto');
const zoho = require('../../apis/zoho_crm');
const path = require('path');
const funcionesCompartidasCtrl = require('./funcionesCompartidasCtrl.js');
const PDFDocument = require('pdfkit');
const axios = require('axios');
//const ftpConfig = require('../config/ftp');
//const jsftp = require("jsftp");
const { json } = require('body-parser');

const nota_cobro_wsc_pdf_Ctrl = require(process.env.RutaQiaoRestServer+'/server/controllers/nota_cobro_wsc_pdf_Ctrl.js');

/*
const Ftp = new jsftp({
    host: ftpConfig.config_FTP.host,
    port: 21, // defaults to 21
    user: ftpConfig.config_FTP.usuario, // defaults to "anonymous"
    pass: ftpConfig.config_FTP.contrasena // defaults to "@anonymous"
});
*/
/*
const ftpConfig = require('../config/ftp');
const jsftp = require("jsftp");
*/
/*
const Ftp = new jsftp({
    host: ftpConfig.config_FTP.host,
    port: 21, // defaults to 21
    user: ftpConfig.config_FTP.usuario, // defaults to "anonymous"
    pass: ftpConfig.config_FTP.contrasena // defaults to "@anonymous"
});
*/

const encryptText = async (texto) => {
    //const secret = 'd6F3Efeq';
    const SECRET = process.env.SECRET;
    let hash = cryptoJs.AES.encrypt(texto.toString(), SECRET).toString();
    hash = new Buffer.from(hash).toString("base64");
    //let hash = Math.floor((Math.random() * 9999999999999)+100000000000).toString().slice(0,12)+texto;
    return hash;
}

exports.ProcesarExcelGatillos = async (req, res) => {
    console.log(".::.");
    console.log(".::.");
    const moment = require('moment');
        console.log(moment().format("DD-MM-YYYY HH:mm")+'\n\n');
    var shc_ProcesarExcelGatillos = require('node-schedule');

    shc_ProcesarExcelGatillos.scheduleJob('*/30 * * * * *', () => {
        function_ProcesarExcelGatillos();

    });

    async function function_ProcesarExcelGatillos(){

        var ExistenDatos = await client.query(` select * FROM public.excel_despachos WHERE mensaje_carga='01.- ARCHIVO CARGADO' `);

        if(ExistenDatos && ExistenDatos.rows && ExistenDatos.rows.length>0)
        {

            await client.query(` update public.excel_despachos set mensaje_carga = concat(mensaje_carga,'\n\n02.- PROCESANDO DATOS PARA REPORTE') `);

            await client.query(` DELETE FROM public.sla_00_completo where estado_despacho!='ENTREGADO' `);

            console.log('.::.');
            console.log('.::.');
            console.log('.::.');
            console.log('Insertando el reporte ');
            await client.query(`
            INSERT INTO public.sla_00_completo(id_proveedor, nombre_proveedor, fecha_creacion_proveedor, id_cliente, razon_social_cliente, ejecutivo, bultos_esperados, m3_esperados, peso_esperado, bultos_recepcionados, bodega_recepcion, fecha_ultima_carga_documentos, fecha_ultima_recepcion, fecha_de_creacion_del_consolidado, fecha_cierre_consolidado_comercial, id_consolidado_comercial, tracking_id, proforma_id, fecha_consolidado_contenedor, fecha_ingreso_datos_contenedor_nave_eta, n_contenedor, despacho_id, nombre_nave, etd_nave_asignada, fecha_nueva_etd_o_eta, eta, n_carpeta, fecha_publicacion, aforo, fecha_aforo, fecha_retiro, hora_retiro, fecha_desconsolidacion_pudahuel, hora_desconsolidacion, estado_finanzas, fecha_de_pago, fecha_ingreso_direccion, fecha_programada, fecha_entrega_retiro, estado_despacho, dias_libres, fecha_creacion_cliente, m3_recibidos) 
            select

            coalesce(prov.id::text, ''::text) AS id_proveedor

            , case when ( tck.fecha_creacion is null or tck.fecha_creacion <= to_date('2022/05/31 23:59:59', 'YYYY/MM/DD HH24:MI') ) and (prov.nombre IS NULL OR prov.nombre='' ) then 'OK' else 
            coalesce(prov.nombre, ''::character varying)
            end as nombre_proveedor

            , case when to_char(tck.fecha_creacion, 'YYYY/MM/DD'::text)='00/01/1900' or tck.fecha_creacion is null then '01/01/2000' else 
            to_char(tck.fecha_creacion, 'DD/MM/YYYY'::text)
            end as fecha_creacion_proveedor

            , case when ( tck.fecha_creacion is null or tck.fecha_creacion <= to_date('2022/05/31 23:59:59', 'YYYY/MM/DD HH24:MI') ) and (cli.id IS NULL ) then '0000' else 
            coalesce(cli.id::text, ''::text)
            end as id_cliente

            , case when ( tck.fecha_creacion is null or tck.fecha_creacion <= to_date('2022/05/31 23:59:59', 'YYYY/MM/DD HH24:MI') ) and (cli."razonSocial" IS NULL or cli."razonSocial"='' ) then 'OK' else 
            coalesce(cli."razonSocial", ''::character varying)
            end as razon_social_cliente

            , case when ( tck.fecha_creacion is null or tck.fecha_creacion <= to_date('2022/05/31 23:59:59', 'YYYY/MM/DD HH24:MI') ) and (comer.nombre IS NULL or comer.nombre='' ) then 'OK' else 
            coalesce(comer.nombre, ''::character varying)
            end as ejecutivo

            , case when ( tck.fecha_creacion is null or tck.fecha_creacion <= to_date('2022/05/31 23:59:59', 'YYYY/MM/DD HH24:MI') ) and (tck.cantidad_bultos IS NULL ) then '0000'::text else 
            coalesce(tck.cantidad_bultos::text, ''::text)
            end as bultos_esperados

            , case when ( tck.fecha_creacion is null or tck.fecha_creacion <= to_date('2022/05/31 23:59:59', 'YYYY/MM/DD HH24:MI') ) and (tck.volumen IS NULL ) then '0000' else 
            REPLACE(coalesce(tck.volumen::text,''), '.', ',')
            end as m3_esperado

            , case when ( tck.fecha_creacion is null or tck.fecha_creacion <= to_date('2022/05/31 23:59:59', 'YYYY/MM/DD HH24:MI') ) and (tck.peso IS NULL ) then '0000' else 
            REPLACE(coalesce(tck.peso::text,''), '.', ',')
            end as peso_esperado

            , case when ( tck.fecha_creacion is null or tck.fecha_creacion <= to_date('2022/05/31 23:59:59', 'YYYY/MM/DD HH24:MI') ) and (SUM(tckd.cantidad) IS NULL ) then '0000' else 
            SUM(tckd.cantidad)::text
            end as bultos_recepcionados

            , case when ( tck.fecha_creacion is null or tck.fecha_creacion <= to_date('2022/05/31 23:59:59', 'YYYY/MM/DD HH24:MI') ) and (bod.nombre IS NULL or bod.nombre='') then 'OK' else 
            bod.nombre
            end as bodega_recepcion

            , case when ( tck.fecha_creacion is null or tck.fecha_creacion <= to_date('2022/05/31 23:59:59', 'YYYY/MM/DD HH24:MI') ) and (tck.fecha_ultima_carga_documento IS NULL) then '01/01/2022' else 
            coalesce(to_char(tck.fecha_ultima_carga_documento, 'DD/MM/YYYY'::text),'')
            end as fecha_ultima_carga_documentos

            , case when ( tck.fecha_creacion is null or tck.fecha_creacion <= to_date('2022/05/31 23:59:59', 'YYYY/MM/DD HH24:MI') ) and (tck.fecha_ultima_recepcion IS NULL or tck.fecha_ultima_recepcion::text='') then '2000/01/01' else 
            coalesce(tck.fecha_ultima_recepcion::text,'')
            end as fecha_ultima_recepcion

            , case when ( tck.fecha_creacion is null or tck.fecha_creacion <= to_date('2022/05/31 23:59:59', 'YYYY/MM/DD HH24:MI') ) and (cons.fecha IS NULL) then '01/01/2000' else 
            to_char(cons.fecha, 'DD/MM/YYYY'::text)
            end as fecha_de_creacion_del_consolidado

            , case when ( tck.fecha_creacion is null or tck.fecha_creacion <= to_date('2022/05/31 23:59:59', 'YYYY/MM/DD HH24:MI') ) and (cons.fecha_cierre IS NULL) then '01/01/2000' else 
            to_char(cons.fecha_cierre, 'DD/MM/YYYY'::text)
            end as fecha_cierre_consolidado_comercial

            , case when ( tck.fecha_creacion is null or tck.fecha_creacion <= to_date('2022/05/31 23:59:59', 'YYYY/MM/DD HH24:MI') ) and (cons.id IS NULL) then '0000' else 
            COALESCE(cons.id::text, ''::text)
            end as id_consolidado_comercial

            , case when ( tck.fecha_creacion is null or tck.fecha_creacion <= to_date('2022/05/31 23:59:59', 'YYYY/MM/DD HH24:MI') ) and (tck.id IS NULL) then '0000' else 
            COALESCE(tck.id::text, ''::text)
            end as tracking_id

            , case when ( tck.fecha_creacion is null or tck.fecha_creacion <= to_date('2022/05/31 23:59:59', 'YYYY/MM/DD HH24:MI') ) and (tck.fk_proforma IS NULL) then '0000' else 
            COALESCE(tck.fk_proforma::text, ''::text)
            end as proforma_id

            , case when ( tck.fecha_creacion is null or tck.fecha_creacion <= to_date('2022/05/31 23:59:59', 'YYYY/MM/DD HH24:MI') ) and (prof.fecha_consolidado IS NULL) then '01/01/2000' else 
            to_char(prof.fecha_consolidado, 'DD/MM/YYYY'::text)
            end as fecha_consolidado_contenedor

            , '' as fecha_ingreso_datos_contenedor_nave_eta

            , case when ( tck.fecha_creacion is null or tck.fecha_creacion <= to_date('2022/05/31 23:59:59', 'YYYY/MM/DD HH24:MI') ) and (tck.fk_contenedor_codigo IS NULL or tck.fk_contenedor_codigo='') then 'OK' else 
            COALESCE(tck.fk_contenedor_codigo::text, ''::text)
            end as n_contenedor

            , '0000' as despacho_id

            , (
            SELECT n2.nave_nombre FROM naves2 n2
            LEFT JOIN naves_eta ne on ne.fk_nave=n2.nave_id
            LEFT JOIN contenedor_viajes_detalle cvd on cvd.fk_nave_eta=ne.id
            LEFT JOIN contenedor_tracking ct on ct.id=cvd.fk_contenedor_tracking
            where ct.id=prof.fk_contenedor_tracking and ne.estado<2 order by ne.id asc limit 1
            ) as nombre_nave

            , case when ( tck.fecha_creacion is null or tck.fecha_creacion <= to_date('2022/05/31 23:59:59', 'YYYY/MM/DD HH24:MI') )
            and (SELECT ne.etd_fecha FROM naves_eta ne LEFT JOIN contenedor_viajes_detalle cvd on cvd.fk_nave_eta=ne.id LEFT JOIN contenedor_tracking ct on ct.id=cvd.fk_contenedor_tracking where ct.id=prof.fk_contenedor_tracking order by ne.id asc limit 1) is null then '01/01/2000' 
            else (SELECT to_char(ne.etd_fecha, 'DD/MM/YYYY'::text) FROM naves_eta ne LEFT JOIN contenedor_viajes_detalle cvd on cvd.fk_nave_eta=ne.id LEFT JOIN contenedor_tracking ct on ct.id=cvd.fk_contenedor_tracking where ct.id=prof.fk_contenedor_tracking order by ne.id asc limit 1)
            end
            as etd_nave_asignada

            , '' fecha_nueva_etd_o_eta

            , case when ( tck.fecha_creacion is null or tck.fecha_creacion <= to_date('2022/05/31 23:59:59', 'YYYY/MM/DD HH24:MI') ) 
            and (SELECT ne.eta_fecha FROM naves_eta ne LEFT JOIN contenedor_viajes_detalle cvd on cvd.fk_nave_eta=ne.id LEFT JOIN contenedor_tracking ct on ct.id=cvd.fk_contenedor_tracking where ct.id=prof.fk_contenedor_tracking order by ne.id desc limit 1) is null then '01/01/2000' 
            else (SELECT to_char(ne.eta_fecha, 'DD/MM/YYYY'::text) FROM naves_eta ne LEFT JOIN contenedor_viajes_detalle cvd on cvd.fk_nave_eta=ne.id LEFT JOIN contenedor_tracking ct on ct.id=cvd.fk_contenedor_tracking where ct.id=prof.fk_contenedor_tracking order by ne.id desc limit 1) 
            end
            as eta

            , case when ( tck.fecha_creacion is null or tck.fecha_creacion <= to_date('2022/05/31 23:59:59', 'YYYY/MM/DD HH24:MI') ) and (desp.n_carpeta IS NULL or desp.n_carpeta='') then 'OK' else 
            COALESCE(desp.n_carpeta::text, ''::text)
            end as n_carpeta

            , case when ( tck.fecha_creacion is null or tck.fecha_creacion <= to_date('2022/05/31 23:59:59', 'YYYY/MM/DD HH24:MI') ) and (exc.listado_aforo IS NULL or exc.listado_aforo='') then '01/01/2000' else 
            COALESCE(exc.listado_aforo, ''::text)
            end as fecha_publicacion

            , case when ( tck.fecha_creacion is null or tck.fecha_creacion <= to_date('2022/05/31 23:59:59', 'YYYY/MM/DD HH24:MI') ) and (exc.aforo IS NULL or exc.aforo='') then 'OK' else 
            COALESCE(exc.aforo, ''::text)
            end as aforo

            , case when ( tck.fecha_creacion is null or tck.fecha_creacion <= to_date('2022/05/31 23:59:59', 'YYYY/MM/DD HH24:MI') ) and (exc.fecha_aforo IS NULL or exc.fecha_aforo='') then '01/01/2022' else 
            COALESCE(exc.fecha_aforo, ''::text)
            end as fecha_aforo

            , case when ( tck.fecha_creacion is null or tck.fecha_creacion <= to_date('2022/05/31 23:59:59', 'YYYY/MM/DD HH24:MI') ) and (exc.eta_tnm IS NULL or exc.eta_tnm='') then '01/01/2022' else 
            COALESCE(exc.eta_tnm, ''::text)
            end as fecha_retiro

            , '' as hora_retiro

            , case when ( tck.fecha_creacion is null or tck.fecha_creacion <= to_date('2022/05/31 23:59:59', 'YYYY/MM/DD HH24:MI') ) and (exc.eta_bodega IS NULL or exc.eta_bodega='') then '01/01/2022' else 
            COALESCE(exc.eta_bodega, ''::text)
            end as fecha_desconsolidacion_pudahuel

            , '' as hora_desconsolidacoin

            , case when ( tck.fecha_creacion is null or tck.fecha_creacion <= to_date('2022/05/31 23:59:59', 'YYYY/MM/DD HH24:MI') ) and (exc.pago_finanzas IS NULL or exc.pago_finanzas='') then 'OK' else 
            COALESCE(exc.pago_finanzas, ''::text)
            end as estado_finanzas

            , case when ( tck.fecha_creacion is null or tck.fecha_creacion <= to_date('2022/05/31 23:59:59', 'YYYY/MM/DD HH24:MI') ) and (exc.ultimo_pago IS NULL or exc.ultimo_pago='') then '01/01/2022' else 
            COALESCE(exc.ultimo_pago, ''::text)
            end as fecha_de_pago

            , '' as fecha_ingreso_direccion

            , '' as fecha_programada

            , case when ( tck.fecha_creacion is null or tck.fecha_creacion <= to_date('2022/05/31 23:59:59', 'YYYY/MM/DD HH24:MI') ) 
			and (select temp1.fecha_entrega from public.tracking_entrega as temp1 where temp1.fk_tracking=tck.id and temp1.fecha_entrega is not null and temp1.estado is true order by id desc limit 1) is null
			and (exc.eta_despacho IS NULL or exc.eta_despacho='') then '01/01/2022' 
			when (select temp1.fecha_entrega from public.tracking_entrega as temp1 where temp1.fk_tracking=tck.id and temp1.fecha_entrega is not null and temp1.estado is true order by id desc limit 1) is not null then 
			to_char((select temp1.fecha_entrega from public.tracking_entrega as temp1 where temp1.fk_tracking=tck.id and temp1.fecha_entrega is not null and temp1.estado is true order by id desc limit 1), 'DD/MM/YYYY'::text)
			else
            COALESCE(exc.eta_despacho, ''::text)
            end as fecha_entrega_retiro

            , case when ( tck.fecha_creacion is null or tck.fecha_creacion <= to_date('2022/05/31 23:59:59', 'YYYY/MM/DD HH24:MI') ) and (exc.estado_despacho IS NULL or exc.estado_despacho='') then 'OK' else 
            COALESCE(exc.estado_despacho, ''::text)
            end as estado_despacho

            , case when ( tck.fecha_creacion is null or tck.fecha_creacion <= to_date('2022/05/31 23:59:59', 'YYYY/MM/DD HH24:MI') ) and (exc.dias_libres IS NULL or exc.dias_libres='') then '0000' else 
            COALESCE(exc.dias_libres, ''::text)
            end as dias_libres

            , case when to_char(to_date(cli."fechaCreacion", 'YYYY/MM/DD'), 'YYYY/MM/DD'::text)='00/01/1900' then '01/01/2000' else to_char(to_date(cli."fechaCreacion", 'YYYY/MM/DD'), 'DD/MM/YYYY'::text) end as fecha_creacion_cliente

            , case when ( tck.fecha_creacion is null or tck.fecha_creacion <= to_date('2022/05/31 23:59:59', 'YYYY/MM/DD HH24:MI') ) and (tck.volumen_recepcionado IS NULL ) then '0000' else 
            REPLACE(coalesce(tck.volumen_recepcionado::text,''), '.', ',')
            end as m3_recibidos

            from
            public.tracking as tck
            inner join public.clientes as cli on cli.id = tck.fk_cliente and cli.valido_reportes='SI'

            left join public.tracking_detalle_agrupado as tckd on tckd.tracking_id=tck.id
            left join public.usuario as comer on cli.fk_comercial = comer.id
            left join public.proveedores as prov on prov.id = tck.fk_proveedor
            left join public.bodegas as bod on bod.id = tck.fk_bodega
            left join public.consolidado_tracking constck on constck.fk_tracking = tck.id
            left join public.consolidado as cons on constck.fk_consolidado = cons.id
            left join public.contenedor_proforma as prof on tck.fk_proforma = prof.id

            left join public.despachos as desp on desp.id = ( SELECT temp1.id FROM despachos temp1 WHERE 
            UPPER(regexp_replace(temp1.contenedor,'\\s','','g'))::text=UPPER(regexp_replace(tck.fk_contenedor_codigo,'\\s','','g'))::text
            AND tck.fk_cliente::text=temp1.fk_cliente::text  ORDER BY temp1.id DESC LIMIT 1)
            left join public.excel_despachos as exc ON 
            tck.fk_cliente::text = UPPER(regexp_replace(exc.id_cliente,'\\s','','g'))::text
            AND UPPER(regexp_replace(desp.n_carpeta,'\\s','','g'))::text = UPPER(regexp_replace(exc.carpeta,'\\s','','g'))::text 
            AND UPPER(regexp_replace(tck.fk_contenedor_codigo,'\\s','','g'))::text = UPPER(regexp_replace(exc.contenedor,'\\s','','g'))::text

            where
            tck.estado_sla_00 is not true 
            and tck.estado>=0 

            group by 
            prov.id
            , tck.fecha_creacion
            , cli.id
            , comer.nombre
            , tck.cantidad_bultos
            , tck.volumen
            , tck.volumen_recepcionado
            , tck.peso
            , bod.nombre
            , tck.fecha_ultima_carga_documento
            , tck.fecha_ultima_recepcion
            , cons.fecha
            , cons.fecha_cierre
            , cons.id
            , tck.id
            , prof.fecha_consolidado
            , prof.fk_contenedor_tracking

            , desp.n_carpeta
            , exc.listado_aforo
            , exc.aforo
            , exc.fecha_aforo
            , exc.eta_tnm
            , exc.eta_bodega
            , exc.pago_finanzas
            , exc.ultimo_pago
            , exc.eta_despacho
            , exc.estado_despacho
            , exc.dias_libres
            `);

            await client.query(`
            UPDATE tracking a SET estado_sla_00 = true WHERE estado_sla_00 is not true and EXISTS (SELECT FROM sla_00_completo b WHERE b.tracking_id::text = a.id::text and b.estado_despacho='ENTREGADO')
            `);

            await client.query(` update public.excel_despachos set mensaje_carga = concat(mensaje_carga,'\n\n03.- CAPTURADO DATOS PARA ARCHIVO DE REPORTE') `);

            console.log('.::.');
            console.log('.::.');
            console.log('.::.');
            console.log('Capturando el porte');
            var Reporte = await client.query(`
            SELECT
            coalesce(id_proveedor,'') as id_proveedor
            , coalesce(nombre_proveedor,'') as nombre_proveedor

            , case when length(fecha_creacion_proveedor)>0 and fecha_creacion_proveedor!='' and fecha_creacion_proveedor is not null and fecha_creacion_proveedor!='#¡REF!' then
            coalesce(to_char(to_date(fecha_creacion_proveedor, 'DD/MM/YYYY'), 'DD/MM/YYYY'),'') else
            '' end as fecha_creacion_proveedor

            , coalesce(id_cliente,'') as id_cliente
            , coalesce(razon_social_cliente,'') as razon_social_cliente
            , coalesce(ejecutivo,'') as ejecutivo_comercial
            , coalesce(ejecutivo,'') as ejecutivo_cuenta
            , coalesce(bultos_esperados::text,'') as bultos_esperados
            , REPLACE(coalesce(m3_esperados::text,''), '.', ',') as m3_esperados
            , REPLACE(coalesce(m3_recibidos::text,''), '.', ',') as m3_recibidos
            , coalesce(peso_esperado::text,'') as peso_esperado
            , coalesce(bultos_recepcionados::text,'') as bultos_recepcionados
            , coalesce(bodega_recepcion,'') as bodega_recepcion

            , case when fecha_ultima_carga_documentos='OK' then fecha_ultima_carga_documentos
            when length(fecha_ultima_carga_documentos)>0 and fecha_ultima_carga_documentos!='' and fecha_ultima_carga_documentos is not null and fecha_ultima_carga_documentos!='#¡REF!' then
            coalesce(to_char(to_date(fecha_ultima_carga_documentos, 'DD/MM/YYYY'), 'DD/MM/YYYY'),'') else

            case when fecha_cierre_consolidado_comercial='OK' then fecha_cierre_consolidado_comercial
            when length(fecha_cierre_consolidado_comercial)>0 and fecha_cierre_consolidado_comercial!='' and fecha_cierre_consolidado_comercial is not null and fecha_cierre_consolidado_comercial!='#¡REF!' then
            coalesce(to_char(to_date(fecha_cierre_consolidado_comercial, 'DD/MM/YYYY'), 'DD/MM/YYYY'),'') else
            '' end

            end as fecha_ultima_carga_documentos

            , coalesce(fecha_aprobacion_documentos,'') as fecha_aprobacion_documentos

            , case when fecha_ultima_recepcion='OK' then fecha_ultima_recepcion
            when length(fecha_ultima_recepcion)>0 and fecha_ultima_recepcion!='' and fecha_ultima_recepcion is not null and fecha_ultima_recepcion!='#¡REF!' then
            coalesce(to_char(to_date(fecha_ultima_recepcion, 'YYYY/MM/DD'), 'DD/MM/YYYY'),'') else
            '' end as fecha_ultima_recepcion_registro_1

            , case when fecha_de_creacion_del_consolidado='OK' then fecha_de_creacion_del_consolidado
            when length(fecha_de_creacion_del_consolidado)>0 and fecha_de_creacion_del_consolidado!='' and fecha_de_creacion_del_consolidado is not null and fecha_de_creacion_del_consolidado!='#¡REF!' then
            coalesce(to_char(to_date(fecha_de_creacion_del_consolidado, 'DD/MM/YYYY'), 'DD/MM/YYYY'),'') else
            '' end as fecha_de_creacion_del_consolidado

            , case when fecha_cierre_consolidado_comercial='OK' then fecha_cierre_consolidado_comercial
            when length(fecha_cierre_consolidado_comercial)>0 and fecha_cierre_consolidado_comercial!='' and fecha_cierre_consolidado_comercial is not null and fecha_cierre_consolidado_comercial!='#¡REF!' then
            coalesce(to_char(to_date(fecha_cierre_consolidado_comercial, 'DD/MM/YYYY'), 'DD/MM/YYYY'),'') else
            '' end as fecha_cierre_consolidado_comercial

            , coalesce(id_consolidado_comercial,'') as id_consolidado_comercial
            , coalesce(tracking_id,'') as tracking_id
            , coalesce(proforma_id,'') as proforma_id

            , case when fecha_consolidado_contenedor='OK' then fecha_consolidado_contenedor
            when length(fecha_consolidado_contenedor)>0 and fecha_consolidado_contenedor!='' and fecha_consolidado_contenedor is not null and fecha_consolidado_contenedor!='#¡REF!' then
            coalesce(to_char(to_date(fecha_consolidado_contenedor, 'DD/MM/YYYY'), 'DD/MM/YYYY'),'') else
            '' end as fecha_consolidado_contenedor

            , case when fecha_ingreso_datos_contenedor_nave_eta='OK' then fecha_ingreso_datos_contenedor_nave_eta
            when length(fecha_ingreso_datos_contenedor_nave_eta)>0 and fecha_ingreso_datos_contenedor_nave_eta!='' and fecha_ingreso_datos_contenedor_nave_eta is not null and fecha_ingreso_datos_contenedor_nave_eta!='#¡REF!' then
            coalesce(to_char(to_date(fecha_ingreso_datos_contenedor_nave_eta, 'YYYY/MM/DD'), 'DD/MM/YYYY'),'') else
            '' end as fecha_ingreso_datos_contenedor_nave_eta

            , coalesce(n_contenedor,'') as n_contenedor
            , coalesce(despacho_id,'') as despacho_id
            , coalesce(nombre_nave,'') as nombre_nave

            , case when etd_nave_asignada='OK' then etd_nave_asignada
            when length(etd_nave_asignada)>0 and etd_nave_asignada!='' and etd_nave_asignada is not null and etd_nave_asignada!='#¡REF!' then
            coalesce(to_char(to_date(etd_nave_asignada, 'DD/MM/YYYY'), 'DD/MM/YYYY'),'') else
            '' end as etd_nave_asignada

            , case when fecha_nueva_etd_o_eta='OK' then fecha_nueva_etd_o_eta
            when length(fecha_nueva_etd_o_eta)>0 and fecha_nueva_etd_o_eta!='' and fecha_nueva_etd_o_eta is not null and fecha_nueva_etd_o_eta!='#¡REF!' then
            coalesce(to_char(to_date(fecha_nueva_etd_o_eta, 'DD/MM/YYYY'), 'DD/MM/YYYY'),'') else
            '' end as fecha_nueva_etd_o_eta

            , case when eta='OK' then eta
            when length(eta)>0 and eta!='' and eta is not null then
            coalesce(to_char(to_date(eta, 'DD/MM/YYYY'), 'DD/MM/YYYY'),'') else
            '' end as eta

            , coalesce(n_carpeta,'') as n_carpeta
            , coalesce(fecha_publicacion_aforo,'') as fecha_publicacion_aforo

            , case when fecha_publicacion='OK' then fecha_publicacion
            when length(fecha_publicacion)>0 and fecha_publicacion!='' and fecha_publicacion is not null and fecha_publicacion!='#¡REF!' then
            coalesce(to_char(to_date(fecha_publicacion, 'DD/MM/YYYY'), 'DD/MM/YYYY'),'') else
            '' end as fecha_publicacion

            , coalesce(aforo,'') as aforo

            , case when fecha_aforo='OK' then fecha_aforo
            when length(fecha_aforo)>0 and fecha_aforo!='' and fecha_aforo is not null and fecha_aforo!='#¡REF!' then
            coalesce(to_char(to_date(fecha_aforo, 'DD/MM/YYYY'), 'DD/MM/YYYY'),'') else
            '' end as fecha_aforo

            /*, case when fecha_retiro='OK' then fecha_retiro
            when length(fecha_retiro)>0 and fecha_retiro!='' and fecha_retiro is not null and fecha_retiro!='#¡REF!' then
            coalesce(to_char(to_date(fecha_retiro, 'DD/MM/YYYY'), 'DD/MM/YYYY'),'') else
            '' end as fecha_retiro*/
            , coalesce(fecha_retiro_puerto,'') as fecha_retiro_puerto
            , coalesce(hora_retiro,'') as hora_retiro

            , case when fecha_desconsolidacion_pudahuel='OK' then fecha_desconsolidacion_pudahuel
            when length(fecha_desconsolidacion_pudahuel)>0 and fecha_desconsolidacion_pudahuel!='' and fecha_desconsolidacion_pudahuel is not null and fecha_desconsolidacion_pudahuel!='#¡REF!'then 
            coalesce(to_char(to_date(fecha_desconsolidacion_pudahuel, 'DD/MM/YYYY'), 'DD/MM/YYYY'),'') else
            '' end as fecha_desconsolidacion_pudahuel

            , coalesce(hora_desconsolidacion,'') as hora_desconsolidacion
            , coalesce(estado_finanzas,'') as estado_finanzas

            , case when fecha_de_pago='OK' then fecha_de_pago
            when length(fecha_de_pago)>0 and fecha_de_pago!='' and fecha_de_pago is not null and fecha_de_pago!='#¡REF!' then
            coalesce(to_char(to_date(fecha_de_pago, 'DD/MM/YYYY'), 'DD/MM/YYYY'),'') else
            '' end as fecha_de_pago_registro_1

            , case when fecha_ingreso_direccion='OK' then fecha_ingreso_direccion
            when length(fecha_ingreso_direccion)>0 and fecha_ingreso_direccion!='' and fecha_ingreso_direccion is not null and fecha_ingreso_direccion!='#¡REF!' then
            coalesce(to_char(to_date(fecha_ingreso_direccion, 'DD/MM/YYYY'), 'DD/MM/YYYY'),'') else
            '' end as fecha_ingreso_direccion

            , case when fecha_programada='OK' then fecha_programada
            when length(fecha_programada)>0 and fecha_programada!='' and fecha_programada is not null and fecha_programada!='#¡REF!' then
            coalesce(to_char(to_date(fecha_programada, 'DD/MM/YYYY'), 'DD/MM/YYYY'),'') else
            '' end as fecha_programada

            , case when fecha_entrega_retiro='OK' then fecha_entrega_retiro
            when length(fecha_entrega_retiro)>0 and fecha_entrega_retiro!='' and fecha_entrega_retiro is not null and fecha_entrega_retiro!='#¡REF!' then
            coalesce(to_char(to_date(fecha_entrega_retiro, 'DD/MM/YYYY'), 'DD/MM/YYYY'),'') else
            '' end as fecha_entrega

            , coalesce(estado_entrega,'') as estado_entrega
            , coalesce(chofer,'') as chofer
            , coalesce(dias_libres,'') as dias_libres

            , coalesce(fecha_creacion_cliente,'') as fecha_creacion_cliente
        
            FROM public.sla_00_completo
            `);

            var xl = require('excel4node');

            var wb = new xl.Workbook();

            var hoja_1 = wb.addWorksheet('Reporte');

            var ajustar_texto = wb.createStyle({
                alignment: { 
                    shrinkToFit: true,
                    wrapText: true
                },
            });

            for(var AuxCol=1; AuxCol<=41; AuxCol++)
            {
                hoja_1.column(AuxCol).setWidth(15);
            }

            var celda_izquierda = wb.createStyle({
                border: {
                    left: {
                        style: 'thin',
                        color: 'black',
                    },
                    top: {
                        style: 'thin',
                        color: 'black',
                    },
                    bottom: {
                        style: 'thin',
                        color: 'black',
                    },
                    outline: false,
                },
            });

            var celda_medio = wb.createStyle({
                border: {
                    top: {
                        style: 'thin',
                        color: 'black',
                    },
                    bottom: {
                        style: 'thin',
                        color: 'black',
                    },
                    outline: false,
                },
            });

            var celda_derecha = wb.createStyle({
                border: {
                    right: {
                        style: 'thin',
                        color: 'black',
                    },
                    top: {
                        style: 'thin',
                        color: 'black',
                    },
                    bottom: {
                        style: 'thin',
                        color: 'black',
                    },
                    outline: false,
                },
            });

            var estilo_titulo = wb.createStyle({
            font: {
                color: '#000000',
                size: 20,
            },
            numberFormat: '$#,##0.00; ($#,##0.00); -',
            });

            var estilo_cabecera = wb.createStyle({
            font: {
                color: '#000000',
                size: 15,
            },
            fill: {
                type: "pattern",
                patternType: "solid",
                bgColor: "#F0F1F2",
                fgColor: "#F0F1F2"
            },
            numberFormat: '$#,##0.00; ($#,##0.00); -',
            });

            var estilo_contenido_texto = wb.createStyle({
            font: {
                color: '#000000',
                size: 12,
            },
            numberFormat: '$#,##0.00; ($#,##0.00); -',
            });

            var estilo_contenido_numero = wb.createStyle({
                font: {
                    color: '#000000',
                    size: 12,
                },
            });

            try {

                var row = 1;
                var col = 1;
                hoja_1.cell(row,col).string('id_proveedor').style(estilo_cabecera).style(celda_izquierda); col++;
                hoja_1.cell(row,col).string('nombre_proveedor').style(estilo_cabecera).style(celda_medio); col++;
                hoja_1.cell(row,col).string('fecha_creacion_proveedor').style(estilo_cabecera).style(celda_medio); col++;
                hoja_1.cell(row,col).string('id_cliente').style(estilo_cabecera).style(celda_medio); col++;
                hoja_1.cell(row,col).string('razon_social_cliente').style(estilo_cabecera).style(celda_medio); col++;
                hoja_1.cell(row,col).string('ejecutivo_comercial').style(estilo_cabecera).style(celda_medio); col++;
                hoja_1.cell(row,col).string('ejecutivo_cuenta').style(estilo_cabecera).style(celda_medio); col++;
                hoja_1.cell(row,col).string('bultos_esperados').style(estilo_cabecera).style(celda_medio); col++;
                hoja_1.cell(row,col).string('m3_esperados').style(estilo_cabecera).style(celda_medio); col++;
                hoja_1.cell(row,col).string('peso_esperado').style(estilo_cabecera).style(celda_medio); col++;
                hoja_1.cell(row,col).string('bultos_recepcionados').style(estilo_cabecera).style(celda_medio); col++;
                hoja_1.cell(row,col).string('bodega_recepcion').style(estilo_cabecera).style(celda_medio); col++;
                hoja_1.cell(row,col).string('fecha_ultima_carga_documentos').style(estilo_cabecera).style(celda_medio); col++;
                hoja_1.cell(row,col).string('fecha_ultima_recepcion_registro_1').style(estilo_cabecera).style(celda_medio); col++;
                hoja_1.cell(row,col).string('fecha_ultima_recepcion_registro_2').style(estilo_cabecera).style(celda_medio); col++;
                hoja_1.cell(row,col).string('fecha_ultima_recepcion_registro_3').style(estilo_cabecera).style(celda_medio); col++;
                hoja_1.cell(row,col).string('fecha_ultima_recepcion_registro_4').style(estilo_cabecera).style(celda_medio); col++;
                hoja_1.cell(row,col).string('fecha_ultima_recepcion_registro_5').style(estilo_cabecera).style(celda_medio); col++;
                hoja_1.cell(row,col).string('fecha_de_creacion_del_consolidado').style(estilo_cabecera).style(celda_medio); col++;
                hoja_1.cell(row,col).string('fecha_cierre_consolidado_comercial').style(estilo_cabecera).style(celda_medio); col++;
                hoja_1.cell(row,col).string('id_consolidado_comercial').style(estilo_cabecera).style(celda_medio); col++;
                hoja_1.cell(row,col).string('tracking_id').style(estilo_cabecera).style(celda_medio); col++;
                hoja_1.cell(row,col).string('proforma_id').style(estilo_cabecera).style(celda_medio); col++;
                hoja_1.cell(row,col).string('fecha_consolidado_contenedor').style(estilo_cabecera).style(celda_medio); col++;
                hoja_1.cell(row,col).string('fecha_ingreso_datos_contenedor_nave_eta').style(estilo_cabecera).style(celda_medio); col++;
                hoja_1.cell(row,col).string('n_contenedor').style(estilo_cabecera).style(celda_medio); col++;
                hoja_1.cell(row,col).string('despacho_id').style(estilo_cabecera).style(celda_medio); col++;
                hoja_1.cell(row,col).string('nombre_nave').style(estilo_cabecera).style(celda_medio); col++;
                hoja_1.cell(row,col).string('etd_nave_asignada').style(estilo_cabecera).style(celda_medio); col++;
                hoja_1.cell(row,col).string('fecha_nueva_etd_o_eta').style(estilo_cabecera).style(celda_medio); col++;
                hoja_1.cell(row,col).string('eta').style(estilo_cabecera).style(celda_medio); col++;
                hoja_1.cell(row,col).string('n_carpeta').style(estilo_cabecera).style(celda_medio); col++;
                hoja_1.cell(row,col).string('fecha_publicacion_aforo').style(estilo_cabecera).style(celda_medio); col++;
            /* hoja_1.cell(row,28).string('fecha_publicacion').style(estilo_cabecera).style(celda_medio); col++;*/
                hoja_1.cell(row,col).string('aforo').style(estilo_cabecera).style(celda_medio); col++;
                hoja_1.cell(row,col).string('fecha_aforo').style(estilo_cabecera).style(celda_medio); col++;
                hoja_1.cell(row,col).string('fecha_retiro_puerto').style(estilo_cabecera).style(celda_medio); col++;
                /*hoja_1.cell(row,31).string('fecha_retiro').style(estilo_cabecera).style(celda_medio); col++;*/
                hoja_1.cell(row,col).string('hora_retiro').style(estilo_cabecera).style(celda_medio); col++;
                hoja_1.cell(row,col).string('fecha_desconsolidacion_pudahuel').style(estilo_cabecera).style(celda_medio); col++;
                hoja_1.cell(row,col).string('hora_desconsolidacion').style(estilo_cabecera).style(celda_medio); col++;
                hoja_1.cell(row,col).string('estado_finanzas').style(estilo_cabecera).style(celda_medio); col++;
                hoja_1.cell(row,col).string('fecha_de_pago_registro_1').style(estilo_cabecera).style(celda_medio); col++;
                hoja_1.cell(row,col).string('fecha_de_pago_registro_2').style(estilo_cabecera).style(celda_medio); col++;
                hoja_1.cell(row,col).string('fecha_de_pago_registro_3').style(estilo_cabecera).style(celda_medio); col++;
                hoja_1.cell(row,col).string('fecha_de_pago_registro_4').style(estilo_cabecera).style(celda_medio); col++;
                hoja_1.cell(row,col).string('fecha_de_pago_registro_5').style(estilo_cabecera).style(celda_medio); col++;
                hoja_1.cell(row,col).string('fecha_solicitud_despacho').style(estilo_cabecera).style(celda_medio); col++;
                hoja_1.cell(row,col).string('fecha_prog_despacho').style(estilo_cabecera).style(celda_medio); col++;
                hoja_1.cell(row,col).string('direccion_entrega').style(estilo_cabecera).style(celda_medio); col++;
                hoja_1.cell(row,col).string('comuna').style(estilo_cabecera).style(celda_medio); col++;
                hoja_1.cell(row,col).string('tipo_entrega').style(estilo_cabecera).style(celda_medio); col++;
                hoja_1.cell(row,col).string('fecha_listo_para_entrega').style(estilo_cabecera).style(celda_medio); col++;
                hoja_1.cell(row,col).string('fecha_ingreso_direccion').style(estilo_cabecera).style(celda_medio); col++;
                hoja_1.cell(row,col).string('fecha_programada').style(estilo_cabecera).style(celda_medio); col++;
                hoja_1.cell(row,col).string('fecha_entrega').style(estilo_cabecera).style(celda_medio); col++;
                hoja_1.cell(row,col).string('estado_entrega').style(estilo_cabecera).style(celda_medio); col++;
                hoja_1.cell(row,col).string('chofer').style(estilo_cabecera).style(celda_medio); col++;
                hoja_1.cell(row,col).string('dias_libres').style(estilo_cabecera).style(celda_medio); col++;
                hoja_1.cell(row,col).string('fecha_creacion_cliente').style(estilo_cabecera).style(celda_medio); col++;
                hoja_1.cell(row,col).string('m3_recibidos').style(estilo_cabecera).style(celda_derecha); col++;

                
                for(var i=0; i<Reporte.rows.length; i++)
                {
                    row++;
                    col = 1;
                    hoja_1.cell(row,col).string(''+Reporte.rows[i]['id_proveedor']+''.toString()).style(estilo_contenido_texto).style(celda_izquierda); col++;
                    hoja_1.cell(row,col).string(''+Reporte.rows[i]['nombre_proveedor']+''.toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                    hoja_1.cell(row,col).string(''+Reporte.rows[i]['fecha_creacion_proveedor']+''.toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                    hoja_1.cell(row,col).string(''+Reporte.rows[i]['id_cliente']+''.toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                    hoja_1.cell(row,col).string(''+Reporte.rows[i]['razon_social_cliente']+''.toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                    hoja_1.cell(row,col).string(''+Reporte.rows[i]['ejecutivo_comercial']+''.toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                    hoja_1.cell(row,col).string(''+Reporte.rows[i]['ejecutivo_cuenta']+''.toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                    hoja_1.cell(row,col).string(''+Reporte.rows[i]['bultos_esperados']+''.toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                    hoja_1.cell(row,col).string(''+Reporte.rows[i]['m3_esperados']+''.toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                    hoja_1.cell(row,col).string(''+Reporte.rows[i]['peso_esperado']+''.toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                    hoja_1.cell(row,col).string(''+Reporte.rows[i]['bultos_recepcionados']+''.toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                    hoja_1.cell(row,col).string(''+Reporte.rows[i]['bodega_recepcion']+''.toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                    hoja_1.cell(row,col).string(''+Reporte.rows[i]['fecha_ultima_carga_documentos']+''.toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                    hoja_1.cell(row,col).string(''+Reporte.rows[i]['fecha_ultima_recepcion_registro_1']+''.toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                    hoja_1.cell(row,col).string('').style(estilo_contenido_texto).style(celda_medio); col++;
                    hoja_1.cell(row,col).string('').style(estilo_contenido_texto).style(celda_medio); col++;
                    hoja_1.cell(row,col).string('').style(estilo_contenido_texto).style(celda_medio); col++;
                    hoja_1.cell(row,col).string('').style(estilo_contenido_texto).style(celda_medio); col++;
                    hoja_1.cell(row,col).string(''+Reporte.rows[i]['fecha_de_creacion_del_consolidado']+''.toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                    hoja_1.cell(row,col).string(''+Reporte.rows[i]['fecha_cierre_consolidado_comercial']+''.toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                    hoja_1.cell(row,col).string(''+Reporte.rows[i]['id_consolidado_comercial']+''.toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                    hoja_1.cell(row,col).string(''+Reporte.rows[i]['tracking_id']+''.toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                    hoja_1.cell(row,col).string(''+Reporte.rows[i]['proforma_id']+''.toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                    hoja_1.cell(row,col).string(''+Reporte.rows[i]['fecha_consolidado_contenedor']+''.toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                    hoja_1.cell(row,col).string(''+Reporte.rows[i]['fecha_ingreso_datos_contenedor_nave_eta']+''.toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                    hoja_1.cell(row,col).string(''+Reporte.rows[i]['n_contenedor']+''.toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                    hoja_1.cell(row,col).string(''+Reporte.rows[i]['despacho_id']+''.toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                    hoja_1.cell(row,col).string(''+Reporte.rows[i]['nombre_nave']+''.toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                    hoja_1.cell(row,col).string(''+Reporte.rows[i]['etd_nave_asignada']+''.toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                    hoja_1.cell(row,col).string(''+Reporte.rows[i]['fecha_nueva_etd_o_eta']+''.toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                    hoja_1.cell(row,col).string(''+Reporte.rows[i]['eta']+''.toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                    hoja_1.cell(row,col).string(''+Reporte.rows[i]['n_carpeta']+''.toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                    hoja_1.cell(row,col).string(''+Reporte.rows[i]['fecha_publicacion_aforo']+''.toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                    hoja_1.cell(row,col).string(''+Reporte.rows[i]['aforo']+''.toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                    hoja_1.cell(row,col).string(''+Reporte.rows[i]['fecha_aforo']+''.toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                    hoja_1.cell(row,col).string(''+Reporte.rows[i]['fecha_retiro_puerto']+''.toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                    hoja_1.cell(row,col).string(''+Reporte.rows[i]['hora_retiro']+''.toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                    hoja_1.cell(row,col).string(''+Reporte.rows[i]['fecha_desconsolidacion_pudahuel']+''.toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                    hoja_1.cell(row,col).string(''+Reporte.rows[i]['hora_desconsolidacion']+''.toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                    hoja_1.cell(row,col).string(''+Reporte.rows[i]['estado_finanzas']+''.toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                    hoja_1.cell(row,col).string(''+Reporte.rows[i]['fecha_de_pago_registro_1']+''.toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                    hoja_1.cell(row,col).string('').style(estilo_contenido_texto).style(celda_medio); col++;
                    hoja_1.cell(row,col).string('').style(estilo_contenido_texto).style(celda_medio); col++;
                    hoja_1.cell(row,col).string('').style(estilo_contenido_texto).style(celda_medio); col++;
                    hoja_1.cell(row,col).string('').style(estilo_contenido_texto).style(celda_medio); col++;
                    hoja_1.cell(row,col).string(''+Reporte.rows[i]['fecha_solicitud_despacho']+''.toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                    hoja_1.cell(row,col).string(''+Reporte.rows[i]['fecha_prog_despacho']+''.toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                    hoja_1.cell(row,col).string(''+Reporte.rows[i]['direccion_entrega']+''.toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                    hoja_1.cell(row,col).string(''+Reporte.rows[i]['comuna']+''.toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                    hoja_1.cell(row,col).string(''+Reporte.rows[i]['tipo_de_entrega']+''.toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                    hoja_1.cell(row,col).string('').style(estilo_contenido_texto).style(celda_medio); col++;
                    hoja_1.cell(row,col).string(''+Reporte.rows[i]['fecha_ingreso_direccion']+''.toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                    hoja_1.cell(row,col).string(''+Reporte.rows[i]['fecha_programada']+''.toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                    hoja_1.cell(row,col).string(''+Reporte.rows[i]['fecha_entrega']+''.toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                    hoja_1.cell(row,col).string(''+Reporte.rows[i]['estado_entrega']+''.toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                    hoja_1.cell(row,col).string(''+Reporte.rows[i]['chofer']+''.toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                    hoja_1.cell(row,col).string(''+Reporte.rows[i]['dias_libres']+''.toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                    hoja_1.cell(row,col).string(''+Reporte.rows[i]['fecha_creacion_cliente']+''.toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                    hoja_1.cell(row,col).string(''+Reporte.rows[i]['m3_recibidos']+''.toString()).style(estilo_contenido_texto).style(celda_derecha); col++;
                }

            } catch (error) {
                await client.query(` update public.excel_despachos set mensaje_carga = concat(mensaje_carga,'\n\n03.- ERROR `+error+`') `);
                console.log("ERROR "+error);
                res.status(400).send({
                    message: "ERROR AL CREAR EXCEL",
                    success:false,
                }); res.end(); res.connection.destroy();
            }

            console.log('.::.');
            console.log('.::.');
            console.log('.::.');
            console.log('Guardando excel ');


            await client.query(` 
            update public.excel_despachos 
            set 
            mensaje_carga = concat(mensaje_carga,'\n\n04.- REPORTE GENERADO') 
            , link_archivo='`+process.env.UrlRemoteServer1+`get_reporte_gatillos'
            `);

            wb.write('./public/files/exceldespachos/reporte_gatillos.xls', async function(err, stats) {
                if (err) 
                {
                    await client.query(` update public.excel_despachos set mensaje_carga = concat(mensaje_carga,'\n\n04.- ERROR `+err+`') `);
                    console.log("ERROR "+err);
                } else {
                }
            });
        }

    }
};

exports.ActualizarCicloVidaServicio = async (req, res) => {
    var nodeSchedule=require('node-schedule');
    nodeSchedule.scheduleJob('0 0 */1 * * *', () => {
        ActualizarCicloVidaServicio()
    });

    async function ActualizarCicloVidaServicio(){

        console.log('\n-------------------------------------');
        console.log('\n-------------------------------------');
        console.log('\n-------------------------------------');

        var ConsolidadosComerciales = await client.query(`
        SELECT
        cabe.fk_servicio
        , cabe.id as fk_propuesta

        FROM public.gc_propuestas_cabeceras as cabe

        where cabe.fk_servicio is not null

        order by cabe.fk_servicio
        desc
        `);

        if(ConsolidadosComerciales && ConsolidadosComerciales.rows && ConsolidadosComerciales.rows.length>0)
        {
            for( var i=0; i<ConsolidadosComerciales.rows.length; i++)
            {
                console.log('\n\nSERVICIO '+ConsolidadosComerciales.rows[i].fk_servicio+'\n\n');
                let Consolidado = await clientExp.query(`
                select
                ser.fk_consolidado
                from public.servicio as ser
                where
                ser.id=`+ConsolidadosComerciales.rows[i].fk_servicio+`
                order by ser.id desc limit 1
                `);

                if(Consolidado && Consolidado.rows && Consolidado.rows.length>0)
                {
                    let ReporteSla00 = await client.query(`
                    SELECT
                    deta.id_consolidado_comercial as fk_consolida

                    , case when deta.fecha_ultima_recepcion is not null and deta.fecha_ultima_recepcion!='' then
                    to_char(to_date(deta.fecha_ultima_recepcion,'YYYY/MM/DD'),'DD/MM/YYYY') else '' end as fecha_recepcion

                    , case when deta.etd_nave_asignada is not null and deta.etd_nave_asignada!='' then
                    to_char(to_date(deta.etd_nave_asignada,'DD/MM/YYYY'),'DD/MM/YYYY') else '' end as fecha_salida

                    , case when deta.eta is not null and deta.eta!='' then
                    to_char(to_date(deta.eta,'DD/MM/YYYY'),'DD/MM/YYYY') else '' end as fecha_arribo

                    , case when deta.fecha_aforo is not null and deta.fecha_aforo!='' then
                    to_char(to_date(deta.fecha_aforo,'DD/MM/YYYY'),'DD/MM/YYYY') else '' end as fecha_aforo

                    , case when deta.fecha_desconsolidacion_pudahuel is not null and deta.fecha_desconsolidacion_pudahuel!='' then
                    to_char(to_date(deta.fecha_desconsolidacion_pudahuel,'DD/MM/YYYY'),'DD/MM/YYYY') else '' end as fecha_disponible

                    , case when deta.fecha_programada is not null and deta.fecha_programada!='' then
                    to_char(to_date(deta.fecha_programada,'DD/MM/YYYY'),'DD/MM/YYYY') else '' end as fecha_despacho

                    , case when deta.fecha_entrega_retiro is not null and deta.fecha_entrega_retiro!='' then
                    to_char(to_date(deta.fecha_entrega_retiro,'DD/MM/YYYY'),'DD/MM/YYYY') else '' end as fecha_entrega

                    FROM public.sla_00_completo as deta
                    inner join public.consolidado as cons on deta.id_consolidado_comercial=cons.id::text

                    where
                    deta.id_consolidado_comercial='`+Consolidado.rows[0].fk_consolidado+`'
                    limit 1
                    `);

                    if(ReporteSla00 && ReporteSla00.rows && ReporteSla00.rows.length>0)
                    {
                        let FechaRecepcion1 = await client.query(`
                        select
                        temp1.fecha
                        from public.servicio_historial as temp1
                        where temp1.fk_servicio=`+ConsolidadosComerciales.rows[i].fk_servicio+` and temp1.texto='Recepción de carga en bodega'
                        order by temp1.fecha desc limit 1
                        `);
                        let FechaSalida1 = await client.query(`
                        select
                        temp1.fecha
                        from public.servicio_historial as temp1
                        where temp1.fk_servicio=`+ConsolidadosComerciales.rows[i].fk_servicio+` and temp1.texto='Salida de puerto de origen'
                        order by temp1.fecha desc limit 1
                        `);
                        let FechaAforo1 = await client.query(`
                        select
                        temp1.fecha
                        from public.servicio_historial as temp1
                        where temp1.fk_servicio=`+ConsolidadosComerciales.rows[i].fk_servicio+` and temp1.texto='Notificación de aforo'
                        order by temp1.fecha desc limit 1
                        `);
                        let FechaArribo1 = await client.query(`
                        select
                        temp1.fecha
                        from public.servicio_historial as temp1
                        where temp1.fk_servicio=`+ConsolidadosComerciales.rows[i].fk_servicio+` and temp1.texto='Retiro de puerto'
                        order by temp1.fecha desc limit 1
                        `);
                        let FechaDisponible1 = await client.query(`
                        select
                        temp1.fecha
                        from public.servicio_historial as temp1
                        where temp1.fk_servicio=`+ConsolidadosComerciales.rows[i].fk_servicio+` and temp1.texto='Carga disponible para entrega'
                        order by temp1.fecha desc limit 1
                        `);
                        let FechaDespacho1 = await client.query(`
                        select
                        temp1.fecha
                        from public.servicio_historial as temp1
                        where temp1.fk_servicio=`+ConsolidadosComerciales.rows[i].fk_servicio+` and temp1.texto='Entrega en ruta'
                        order by temp1.fecha desc limit 1
                        `);
                        let FechaEntregado1 = await client.query(`
                        select
                        temp1.fecha
                        from public.servicio_historial as temp1
                        where temp1.fk_servicio=`+ConsolidadosComerciales.rows[i].fk_servicio+` and temp1.texto='Carga entregada'
                        order by temp1.fecha desc limit 1
                        `);

                        if( FechaRecepcion1.rows.length<=0 && ReporteSla00.rows[0].fecha_recepcion!='' )
                        {
                            console.log('\n\nSERVICIO Y RECEPCION '+ReporteSla00.rows[0].fecha_recepcion+'\n\n');
                            await client.query(`
                            INSERT INTO public.servicio_historial(
                            texto, fk_servicio, fk_usuario, fecha, estado)
                            VALUES ('Recepción de carga en bodega', `+ConsolidadosComerciales.rows[i].fk_servicio+`, 1, '`+ReporteSla00.rows[0].fecha_recepcion+`', true);
                            `);
                        }

                        if( FechaSalida1.rows.length<=0 && ReporteSla00.rows[0].fecha_salida!='' )
                        {
                            console.log('\n\nSERVICIO Y SALIDA '+ReporteSla00.rows[0].fecha_salida+'\n\n');
                            await client.query(`
                            INSERT INTO public.servicio_historial(
                            texto, fk_servicio, fk_usuario, fecha, estado)
                            VALUES ('Salida de puerto de origen', `+ConsolidadosComerciales.rows[i].fk_servicio+`, 1, '`+ReporteSla00.rows[0].fecha_salida+`', true);
                            `);
                        }

                        if( FechaArribo1.rows.length<=0 && ReporteSla00.rows[0].fecha_arribo!='' )
                        {
                            console.log('\n\nSERVICIO Y ARRIBO '+ReporteSla00.rows[0].fecha_arribo+'\n\n');
                            await client.query(`
                            INSERT INTO public.servicio_historial(
                            texto, fk_servicio, fk_usuario, fecha, estado)
                            VALUES ('Retiro de puerto', `+ConsolidadosComerciales.rows[i].fk_servicio+`, 1, '`+ReporteSla00.rows[0].fecha_arribo+`', true);
                            `);
                        }

                        if( FechaAforo1.rows.length<=0 && ReporteSla00.rows[0].fecha_aforo!='' )
                        {
                            console.log('\n\nSERVICIO Y AFORO '+ReporteSla00.rows[0].fecha_aforo+'\n\n');
                            await client.query(`
                            INSERT INTO public.servicio_historial(
                            texto, fk_servicio, fk_usuario, fecha, estado)
                            VALUES ('Notificación de aforo', `+ConsolidadosComerciales.rows[i].fk_servicio+`, 1, '`+ReporteSla00.rows[0].fecha_aforo+`', true);
                            `);
                        }

                        if( FechaDisponible1.rows.length<=0 && ReporteSla00.rows[0].fecha_disponible!='' )
                        {
                            console.log('\n\nSERVICIO Y DIPONIBLE ENTREGA '+ReporteSla00.rows[0].fecha_disponible+'\n\n');
                            await client.query(`
                            INSERT INTO public.servicio_historial(
                            texto, fk_servicio, fk_usuario, fecha, estado)
                            VALUES ('Carga disponible para entrega', `+ConsolidadosComerciales.rows[i].fk_servicio+`, 1, '`+ReporteSla00.rows[0].fecha_disponible+`', true);
                            `);
                        }

                        if( FechaDespacho1.rows.length<=0 && ReporteSla00.rows[0].fecha_despacho!='' )
                        {
                            console.log('\n\nSERVICIO Y RUTA '+ReporteSla00.rows[0].fecha_despacho+'\n\n');
                            await client.query(`
                            INSERT INTO public.servicio_historial(
                            texto, fk_servicio, fk_usuario, fecha, estado)
                            VALUES ('Entrega en ruta', `+ConsolidadosComerciales.rows[i].fk_servicio+`, 1, '`+ReporteSla00.rows[0].fecha_despacho+`', true);
                            `);
                        }

                        if( FechaEntregado1.rows.length<=0 && ReporteSla00.rows[0].fecha_entrega!='' )
                        {
                            console.log('\n\nSERVICIO Y ENTREGADO '+ReporteSla00.rows[0].fecha_entrega+'\n\n');
                            await client.query(`
                            INSERT INTO public.servicio_historial(
                            texto, fk_servicio, fk_usuario, fecha, estado)
                            VALUES ('Carga entregada', `+ConsolidadosComerciales.rows[i].fk_servicio+`, 1, '`+ReporteSla00.rows[0].fecha_entrega+`', true);
                            `);
                        }
                    }
                }
            }
        }
    }
};

exports.cargar_consolidados_expdigital = async (req, res) => {
    var nodeSchedule=require('node-schedule');

    nodeSchedule.scheduleJob('*/10 * * * *', () => {
        funcion_cargar_consolidados_expdigital()
    });

    async function funcion_cargar_consolidados_expdigital(){

        let results=await client.query(`
        SELECT 
        gc.id as fk_propuesta
        , gc.fk_servicio
        , c.id as fk_consolidado
        , gc.fk_cliente
        , cl.fk_comercial
        , to_char(c.fecha, 'DD/MM/YYYY') as fecha_creacion
        FROM
        public.gc_propuestas_cabeceras gc
        INNER JOIN public.consolidado c on c.fk_propuesta=gc.id
        INNER JOIN public.clientes cl on cl.id=gc.fk_cliente
        WHERE 
        gc.fk_servicio is null
        /* and to_char(c.fecha, 'DD/MM/YYYY') = to_char(now()::date, 'DD/MM/YYYY') */
        order by 
        c.id 
        desc
        limit 1
        `);

        if(results && results.rows && results.rows.length>0)
        {
            let tarifas=await client.query(`
            SELECT 
            "fk_zonaOrigen"
            ,"fk_zonaDestino"
            ,"valorUnitarioUsd"
            ,"tarifaUsd"
            ,"cmbPeso"
            ,"volumenEstimado"
            ,"pesoEstimado"
            ,"valorBaseUsd"
            ,"unidadesACobrar" 
            FROM public.gc_propuestas_tarifas 
            WHERE 
            fk_cabecera=`+results.rows[0].fk_propuesta
            );

            let trackings=await client.query(`SELECT 
            t.id,
            t.cantidad_bultos,
            t.peso,
            t.volumen,
            t.fk_proveedor,
            CASE WHEN t.packing_list1 IS NOT NULL THEN TRUE ELSE FALSE END AS packing_list1,
            CASE WHEN t.invoice1 IS NOT NULL THEN TRUE ELSE FALSE END AS invoice1,
            p.nombre as fk_proveedor_nombre,
            p."nombreChi" as fk_proveedor_nombre_chino
            FROM public.tracking t 
            INNER JOIN public.consolidado_tracking ct on ct.fk_tracking=t.id
            INNER JOIN public.proveedores p on p.id=t.fk_proveedor
            where ct.fk_consolidado=`+results.rows[0].fk_consolidado+` 
            and t.estado>=0
            `);

            let bultos=0;

            if(trackings && trackings.rows && trackings.rows.length>0)
            {
                trackings.rows.map(item=>{
                    bultos+=parseInt(item.cantidad_bultos);
                })
            }

            let pesoEstimado=0;
            let volumenEstimado=0;
            let valorUnitarioUsd=0;
            let tarifaUsd=0;
            let valorBaseUsd=0;
            let unidadesACobrar=0;
            let fk_zonaOrigen=0;
            let fk_zonaDestino=0;
            let cmbPeso=0;

            if(tarifas && tarifas.rows && tarifas.rows.length>0)
            {
                for(let z=0;z<tarifas.rows.length;z++){
                    if(z==0){
                        pesoEstimado=tarifas.rows[z].pesoEstimado;
                        volumenEstimado=tarifas.rows[z].volumenEstimado;
                        valorUnitarioUsd=tarifas.rows[z].valorUnitarioUsd;
                        valorBaseUsd=tarifas.rows[z].valorBaseUsd;
                        unidadesACobrar=tarifas.rows[z].unidadesACobrar;
                        fk_zonaOrigen=tarifas.rows[z].fk_zonaOrigen;
                        fk_zonaDestino=tarifas.rows[z].fk_zonaDestino;
                        cmbPeso=tarifas.rows[z].cmbPeso;
                    }
                    tarifaUsd+=tarifas.rows[z].tarifaUsd;
                }
            }

            let servicio={
                fk_propuesta:results.rows[0].fk_propuesta,
                fk_consolidado:results.rows[0].fk_consolidado,
                fk_cliente:results.rows[0].fk_cliente,
                fk_comercial:results.rows[0].fk_comercial,
                fk_servicio:results.rows[0].fk_servicio,
                pesoEstimado:pesoEstimado,
                volumenEstimado:volumenEstimado,
                valorUnitarioUsd:valorUnitarioUsd,
                tarifaUsd:tarifaUsd,
                valorBaseUsd:valorBaseUsd,
                unidadesACobrar:unidadesACobrar,
                fk_zonaOrigen:fk_zonaOrigen,
                fk_zonaDestino:fk_zonaDestino,
                cmbPeso:cmbPeso,
                bultos:bultos,
                trackings:trackings.rows
            }

            console.log(`\n\nSe deberia cargar este consolidado\n\n`+JSON.stringify(servicio)+`\n\n`);

            var ServicioId = await funcionesCompartidasCtrl.GuardarServicioEnExperienciaDigital(servicio);

            client.query(`UPDATE public.gc_propuestas_cabeceras SET fk_servicio=`+ServicioId.id+` WHERE id=`+servicio.fk_propuesta);
        }
    }
};

exports.activar_cve_mandato = async (req, res) => {
    var nodeSchedule=require('node-schedule');
    nodeSchedule.scheduleJob('*/60 * * * * *', () => {
        funcion_activar_cve_mandato()
    });

    async function funcion_activar_cve_mandato(){

        console.log('\n-------------------------------------');
        console.log('\n-------------------------------------');
        console.log('\n-------------------------------------');

        var fecha =  new Date();
        var moment = require('moment'); moment.locale('es');
        fecha = moment(fecha).format('DD-MM-YYYY');

        var InfoCliente = await client.query(`
        SELECT
        cli.*
        , coalesce(mand.mandato_id,'') as mandato_id_firma
        FROM public.clientes cli
        left join public.clientes_mandatos as mand on cli.id=mand.fk_cliente and mand.id=(select temp1.id from public.clientes_mandatos as temp1 where temp1.fk_cliente=cli.id order by temp1.id desc limit 1)
        WHERE
        cli.firma_digital_estado='FIRMADO'
        and cli.mandato_ruta is not null
        and cli.cve_verification_allowed is not true
        and to_date(cli.firma_digital_actividad, 'DD/MM/YYYY') <= now()::date - INTERVAL '2 day'
        order by id asc limit 1
        `);

        if(InfoCliente && InfoCliente.rows && InfoCliente.rows.length>0)
        {
            console.log('\nREVISANDO CLIENTE '+InfoCliente.rows[0].id+' \nCEDULA 1: '+InfoCliente.rows[0].cedula_1_ruta+' \nCEDULA 2: '+InfoCliente.rows[0].cedula_2_ruta+' \nPODER SIMPLE: '+InfoCliente.rows[0].podersimple_1_ruta+' \nFIRMADO: '+InfoCliente.rows[0].firma_digital_estado+' \nMANDATO: '+InfoCliente.rows[0].mandato_ruta);

            var ClinteId        = InfoCliente.rows[0].id;
            var Extencion       = null;
            var Nombre1         = null;
            var Nombre2         = null;
            var Data            = null;
            var NombreDato      = null;

            Extencion       = '.pdf';
            Nombre1         = ClinteId+'_mandato.pdf';
            Nombre2         = ClinteId+'_mandato.pdf';
            Data            = null;
            NombreDato      = 'mandato_ruta';

            var Cliente_Mandato = InfoCliente.rows[0].mandato_id_firma;

            const FirmaYaConfig = {
                headers: {
                    'Authorization': 'eyJhbGciOiJIUzI1NiJ9.eyJ1c2VyX2lkIjp7IiRvaWQiOiI2MmNjMmEwMmVkZjQxNTc1NmM4ZmM2ZTUifX0.NWvyrYfkYWRbzlrXTAWHRvwhRt4BqIVGsGk4mg7QSCc',
                    'Content-Type': 'application/json',
                },
            };

            var FirmaYaData = {
                mxml_id: Cliente_Mandato,
            };

            console.log('CONSULTANDO POR : '+JSON.stringify(FirmaYaData));

            var RequestAxiosCve = await axios.post('https://firmaya.idok.cl/api/corp/mxmls/turn_on_cve_verification', JSON.parse(JSON.stringify(FirmaYaData)), FirmaYaConfig);
            console.log("\n\nDETALLE RESPUESTA ACTIVAR CVE DOCUMENTO "+JSON.stringify(RequestAxiosCve.data));

            if( typeof RequestAxiosCve !== 'undefined' )
            {
                if( RequestAxiosCve.status=='200' )
                {
                    if( RequestAxiosCve.data.cve_verification_allowed!=true )
                    {
                        await client.query(`
                        UPDATE
                        public.clientes
                        SET
                        firma_digital_actividad='`+fecha+`'
                        WHERE
                        id=`+ClinteId+`
                        `);
                    }
                    else
                    {
                        await client.query(`
                        UPDATE
                        public.clientes
                        SET
                        firma_digital_actividad='`+fecha+`'
                        , cve_verification_allowed = true
                        WHERE
                        id=`+ClinteId+`
                        `);
                    }

                }
                else
                {
                    await client.query(`
                    UPDATE
                    public.clientes
                    SET
                    firma_digital_actividad='`+fecha+`'
                    WHERE
                    id=`+ClinteId+`
                    `);
                }
            }
            else
            {
                await client.query(`
                UPDATE
                public.clientes
                SET
                firma_digital_actividad='`+fecha+`'
                WHERE
                id=`+ClinteId+`
                `);
            }
        }

        async function CrearArchivo(Ruta1, Ruta2, Nombre1, Nombre2, Data, NombreDato, ClinteId, Extencion)
        {
            try {

                console.log('\nCREANDO ARCHIVO '+Ruta1+Nombre1);

                var BufferArchivo   = new Buffer.from(Data, 'base64');
                console.log('\nLARGO ARCHIVO '+BufferArchivo.length);

                if ( BufferArchivo.length<=0 )
                {
                    ActualizarRuta(NombreDato, 'ERROR', ClinteId);
                }
                else
                {
                    await fs.writeFile(Ruta1+Nombre1, BufferArchivo, err =>
                    {
                        if (err)
                        {
                            console.log('\nERROR CREANDO ARCHIVO '+err);
                            ActualizarRuta(NombreDato, 'ERROR', ClinteId);
                        }
                        else
                        {
                            console.log('\nARCHIVO OK '+Ruta1+Nombre1);

                            if( Extencion=='.TIFF' || Extencion=='.JPEG' || Extencion=='.PNG' || Extencion=='.JPG' )
                            {
                                console.log('\nDETECTADO COMO IMAGEN');
                                ImagenToPdf(Ruta1, Nombre1, Ruta2, Nombre2, NombreDato, ClinteId);
                            }
                            else if( Extencion=='.DOCX' )
                            {
                                console.log('\nDETECTADO COMO DOCX');
                                DocxToPdf(Ruta1, Nombre1, Ruta2, Nombre2, NombreDato, ClinteId);
                            }
                            else if( Extencion=='.PDF' )
                            {
                                console.log('\nDETECTADO COMO PDF');
                                ActualizarRuta(NombreDato, Ruta2+Nombre2, ClinteId);
                            }
                            else
                            {
                                ActualizarRuta(NombreDato, 'ERROR', ClinteId);
                            }

                        }
                    });
                }

            }
            catch (err) { ActualizarRuta(NombreDato, 'ERROR', ClinteId); }
        }

        async function DocxToPdf(Ruta1, Nombre1, Ruta2, Nombre2, NombreDato, ClinteId)
        {
            try{
                console.log('\nDOCX A PDF DE '+Ruta1+Nombre1);
                var docxConverter = require('docx-pdf');
                docxConverter(Ruta1+Nombre1, Ruta1+Nombre2,function(err,result){
                    if(err)
                    {
                        ActualizarRuta(NombreDato, 'ERROR', ClinteId);
                    }
                    else
                    {
                        ActualizarRuta(NombreDato, Ruta2+Nombre2, ClinteId);
                    }

                    fs.unlink(Ruta1+Nombre1, (err) => {
                        if (err)
                        {
                            throw err;
                        }
                        else
                        {
                            console.log('\nDOCX A PDF CORRECTO '+Ruta2+Nombre2);
                        }
                    });
                });
            } catch(err) { ActualizarRuta(NombreDato, 'ERROR', ClinteId);  }
        }

        async function ImagenToPdf(Ruta1, Nombre1, Ruta2, Nombre2, NombreDato, ClinteId)
        {
            try{
                console.log('\nIMAGEN A PDF DE '+Ruta1+Nombre1);
                const doc = new PDFDocument({size: 'LETTER'});
                doc.pipe(fs.createWriteStream(Ruta1+Nombre2));
                doc.image(Ruta1+Nombre1, 20, 20, {fit: [570, 700], align: 'center', valign: 'center'})
                doc.end();
                ActualizarRuta(NombreDato, Ruta2+Nombre2, ClinteId);
                fs.unlink(Ruta1+Nombre1, (err) => {
                    if (err)
                    {
                        throw err;
                    }
                    else
                    {
                        console.log('\nIMAGEN A PDF CORRECTO '+Ruta2+Nombre2);
                    }
                });
            } catch(err) { ActualizarRuta(NombreDato, 'ERROR', ClinteId);  }
        }

        async function ActualizarRuta(Dato, Ruta, CLienteId)
        {
            client.query(`UPDATE public.clientes SET `+Dato+`='`+Ruta+`' where id=`+CLienteId);
        }
    }
};

exports.documentos_clientes = async (req, res) => {
    var nodeSchedule=require('node-schedule');
    nodeSchedule.scheduleJob('*/10 * * * * *', () => {
        funcion_documentos_clientes()
    });

    async function funcion_documentos_clientes(){

        console.log('\n-------------------------------------');
        console.log('\n-------------------------------------');
        console.log('\n-------------------------------------');
        var Ruta1 = process.env.QiaoRestServerPublic+'files/clientes_archivos/';
        var Ruta2 = 'files/clientes_archivos/';

        var InfoCliente = await client.query(`
        SELECT
        cli.*
        , coalesce(mand.mandato_id,'') as mandato_id_firma
        FROM public.clientes cli
        left join public.clientes_mandatos as mand on cli.id=mand.fk_cliente and mand.id=(select temp1.id from public.clientes_mandatos as temp1 where temp1.fk_cliente=cli.id order by temp1.id desc limit 1)
        WHERE
        ( cli.cedula_1_ext is not null and cli.cedula_1_ruta is null )
        or ( cli.cedula_2_ext is not null and cli.cedula_2_ruta is null )
        or ( cli.podersimple_1_ext is not null and cli.podersimple_1_ruta is null )
        or ( cli.firma_digital_estado='FIRMADO' and cli.mandato_ruta is null )
        order by id asc limit 1
        `);

        if(InfoCliente && InfoCliente.rows && InfoCliente.rows.length>0)
        {
            console.log('\nREVISANDO CLIENTE '+InfoCliente.rows[0].id+' \nCEDULA 1: '+InfoCliente.rows[0].cedula_1_ruta+' \nCEDULA 2: '+InfoCliente.rows[0].cedula_2_ruta+' \nPODER SIMPLE: '+InfoCliente.rows[0].podersimple_1_ruta+' \nFIRMADO: '+InfoCliente.rows[0].firma_digital_estado+' \nMANDATO: '+InfoCliente.rows[0].mandato_ruta);

            var ClinteId        = InfoCliente.rows[0].id;
            var Extencion       = null;
            var Nombre1         = null;
            var Nombre2         = null;
            var Data            = null;
            var NombreDato      = null;

            if( InfoCliente.rows[0].cedula_1_ext!=null && InfoCliente.rows[0].cedula_1_ruta == null )
            {
                Extencion       = InfoCliente.rows[0].cedula_1_ext.toUpperCase();
                Nombre1         = ClinteId+'_cedula_1'+InfoCliente.rows[0].cedula_1_ext;
                Nombre2         = ClinteId+'_cedula_1.pdf';
                Data            = InfoCliente.rows[0].cedula_1;
                NombreDato      = 'cedula_1_ruta';
                console.log('\nCREANDO CEDULA 1');
                await CrearArchivo(Ruta1, Ruta2, Nombre1, Nombre2, Data, NombreDato, ClinteId, Extencion);
            }

            if( InfoCliente.rows[0].cedula_2_ext!=null && InfoCliente.rows[0].cedula_2_ruta == null )
            {
                Extencion       = InfoCliente.rows[0].cedula_2_ext.toUpperCase();
                Nombre1         = ClinteId+'_cedula_2'+InfoCliente.rows[0].cedula_2_ext;
                Nombre2         = ClinteId+'_cedula_2.pdf';
                Data            = InfoCliente.rows[0].cedula_2;
                NombreDato      = 'cedula_2_ruta';
                console.log('\nCREANDO CEDULA 2');
                await CrearArchivo(Ruta1, Ruta2, Nombre1, Nombre2, Data, NombreDato, ClinteId, Extencion);
            }

            if( InfoCliente.rows[0].podersimple_1_ext!=null && InfoCliente.rows[0].podersimple_1_ruta == null )
            {
                Extencion       = InfoCliente.rows[0].podersimple_1_ext.toUpperCase();
                Nombre1         = ClinteId+'_podersimple'+InfoCliente.rows[0].podersimple_1_ext;
                Nombre2         = ClinteId+'_podersimple.pdf';
                Data            = InfoCliente.rows[0].podersimple_1;
                NombreDato      = 'podersimple_1_ruta';
                console.log('\nCREANDO PODER SIMPLE');
                await CrearArchivo(Ruta1, Ruta2, Nombre1, Nombre2, Data, NombreDato, ClinteId, Extencion);
            }

            if( InfoCliente.rows[0].firma_digital_estado=='FIRMADO' && InfoCliente.rows[0].mandato_ruta==null )
            {
                Extencion       = '.pdf';
                Nombre1         = ClinteId+'_mandato.pdf';
                Nombre2         = ClinteId+'_mandato.pdf';
                Data            = null;
                NombreDato      = 'mandato_ruta';

                var Cliente_Mandato = InfoCliente.rows[0].mandato_id_firma;

                const FirmaYaConfig = {
                    headers: {
                        'Authorization': 'eyJhbGciOiJIUzI1NiJ9.eyJ1c2VyX2lkIjp7IiRvaWQiOiI2MmNjMmEwMmVkZjQxNTc1NmM4ZmM2ZTUifX0.NWvyrYfkYWRbzlrXTAWHRvwhRt4BqIVGsGk4mg7QSCc',
                        'Content-Type': 'application/json',
                    },
                };

                var FirmaYaData = {
                    mxml_id: Cliente_Mandato,
                };

                console.log('CONSULTANDO POR : '+JSON.stringify(FirmaYaData));

                var RequestAxiosDocumento = await axios.post('https://firmaya.idok.cl/api/corp/groups/download_document', JSON.parse(JSON.stringify(FirmaYaData)), FirmaYaConfig);
                console.log("\n\nDETALLE RESPUESTA CONSULTA ESTADO DOCUMENTO "+JSON.stringify(RequestAxiosDocumento.data));

                var mandato = Ruta1+Nombre1;

                var buf_mandato = new Buffer.from(RequestAxiosDocumento.data.pdf64, 'base64');
                await fs.writeFile(mandato, buf_mandato, err => {
                    if (err)
                    {
                        console.log('\nERROR CREANDO ARCHIVO '+err);

                        ActualizarRuta('mandato_ruta', 'ERROR', ClinteId); 
                    }
                    else
                    {
                        console.log('Saved!');
                        ActualizarRuta('mandato_ruta', 'files/clientes_archivos/'+Nombre1, ClinteId); 
                    }
                });
            }
            else
            {
                ActualizarRuta('mandato_ruta', 'ERROR', ClinteId); 
            }
        }

        async function CrearArchivo(Ruta1, Ruta2, Nombre1, Nombre2, Data, NombreDato, ClinteId, Extencion)
        {
            try {

                console.log('\nCREANDO ARCHIVO '+Ruta1+Nombre1);

                var BufferArchivo   = new Buffer.from(Data, 'base64');
                console.log('\nLARGO ARCHIVO '+BufferArchivo.length);

                if ( BufferArchivo.length<=0 ) 
                {
                    ActualizarRuta(NombreDato, 'ERROR', ClinteId);
                }
                else
                {
                    await fs.writeFile(Ruta1+Nombre1, BufferArchivo, err => 
                    {
                        if (err)  
                        { 
                            console.log('\nERROR CREANDO ARCHIVO '+err);
                            ActualizarRuta(NombreDato, 'ERROR', ClinteId); 
                        }
                        else 
                        {
                            console.log('\nARCHIVO OK '+Ruta1+Nombre1);

                            if( Extencion=='.TIFF' || Extencion=='.JPEG' || Extencion=='.PNG' || Extencion=='.JPG' )
                            {
                                console.log('\nDETECTADO COMO IMAGEN');
                                ImagenToPdf(Ruta1, Nombre1, Ruta2, Nombre2, NombreDato, ClinteId);
                            }
                            else if( Extencion=='.DOCX' )
                            {
                                console.log('\nDETECTADO COMO DOCX');
                                DocxToPdf(Ruta1, Nombre1, Ruta2, Nombre2, NombreDato, ClinteId);
                            }
                            else if( Extencion=='.PDF' )
                            {
                                console.log('\nDETECTADO COMO PDF');
                                ActualizarRuta(NombreDato, Ruta2+Nombre2, ClinteId); 
                            }
                            else
                            {
                                ActualizarRuta(NombreDato, 'ERROR', ClinteId);
                            }

                        }
                    });
                }

            } 
            catch (err) { ActualizarRuta(NombreDato, 'ERROR', ClinteId); }
        }

        async function DocxToPdf(Ruta1, Nombre1, Ruta2, Nombre2, NombreDato, ClinteId)
        {
            try{
                console.log('\nDOCX A PDF DE '+Ruta1+Nombre1);
                var docxConverter = require('docx-pdf');
                docxConverter(Ruta1+Nombre1, Ruta1+Nombre2,function(err,result){
                    if(err)
                    { 
                        ActualizarRuta(NombreDato, 'ERROR', ClinteId); 
                    }
                    else
                    {
                        ActualizarRuta(NombreDato, Ruta2+Nombre2, ClinteId); 
                        fs.unlink(Ruta1+Nombre1, (err) => {
                            if (err) 
                            {
                                throw err;
                            }
                            else
                            {
                                console.log('\nDOCX A PDF CORRECTO '+Ruta2+Nombre2);
                            }
                        });
                    }
                });
            } catch(err) { ActualizarRuta(NombreDato, 'ERROR', ClinteId);  }
        }

        async function ImagenToPdf(Ruta1, Nombre1, Ruta2, Nombre2, NombreDato, ClinteId)
        {
            try{
                console.log('\nIMAGEN A PDF DE '+Ruta1+Nombre1);
                const doc = new PDFDocument({size: 'LETTER'});
                doc.pipe(fs.createWriteStream(Ruta1+Nombre2));
                doc.image(Ruta1+Nombre1, 20, 20, {fit: [570, 700], align: 'center', valign: 'center'})
                doc.end();
                ActualizarRuta(NombreDato, Ruta2+Nombre2, ClinteId); 
                fs.unlink(Ruta1+Nombre1, (err) => {
                    if (err) 
                    {
                        throw err;
                    }
                    else
                    {
                        console.log('\nIMAGEN A PDF CORRECTO '+Ruta2+Nombre2);
                    }
                });
            } catch(err) { ActualizarRuta(NombreDato, 'ERROR', ClinteId);  }
        }

        async function ActualizarRuta(Dato, Ruta, CLienteId)
        {
            client.query(`UPDATE public.clientes SET `+Dato+`='`+Ruta+`' where id=`+CLienteId);
        }
    }
};

exports.firma_digital_enviar_enrolamiento = async (req, res) => {
    var nodeSchedule=require('node-schedule');
    nodeSchedule.scheduleJob('*/60 * * * * *', () => {
        funcion_firma_digital_enviar_enrolamiento()
    });

    async function funcion_firma_digital_enviar_enrolamiento(){
      console.log('tarea programada - correo enrolamiento');
      await funcionesCompartidasCtrl.FirmaDigital_EnviarEnrolamiento();
    }
};
  
  exports.firma_digital_consultar_estado = async (req, res) => {
    var nodeSchedule=require('node-schedule');
    nodeSchedule.scheduleJob('*/60 * * * * *', () => {
        funcion_firma_digital_consultar_estado()
    });

    async function funcion_firma_digital_consultar_estado(){
      await funcionesCompartidasCtrl.FirmaDigital_ConsultarEstado();
    }
};

exports.mail_envios_server2 = async (req, resp) => {
    console.log(".::.");
    console.log(".::.");
    console.log("CONSULTANDO CORREOS 2 ");
    var sche_mail_envios_server2 = require('node-schedule');

    sche_mail_envios_server2.scheduleJob('*/15 * * * * *', () => {
        console.log("ENTRO EN EL CRON JOB ");
        mail_envios_server2();

    });

    async function mail_envios_server2()
    {
        console.log(`
        \n\n
        SELECT
        *
        , 
        case when tipo='mail_notificacion_pago' then coalesce(datos_adicionales,'{}')::jsonb
        else '{}' end as datos_pagos
        FROM
        public.email_envios_logs
        where
        estado='PENDIENTE'
        and id>8900
        order by id
        asc limit 1
        `);

        var Correo = await client.query(`
        SELECT
        *
        , 
        case when tipo='mail_notificacion_pago' then coalesce(datos_adicionales,'{}')::jsonb
        else '{}' end as datos_pagos
        FROM
        public.email_envios_logs
        where
        estado='PENDIENTE'
        and id>8900
        order by id
        asc limit 1
        `);
        var email = '-.com'
        function validateEmail(email)
        {
            var re = /\S+@\S+\.\S+/;
            return re.test(email);
        }

        var EstadoCorreo = null;
        
        if(Correo.rows.length>0)
        {
            console.log("tipo "+Correo.rows[0]['tipo']);
            console.log("tipo_id "+Correo.rows[0]['tipo_id']);
            console.log(".::.");
            console.log(".::.");
            console.log("\n\n\nENVIANDO ID "+Correo.rows[0]['id']);
            console.log("\n\n\nENVIANDO TIPO "+Correo.rows[0]['tipo_id']);
            
            if( Correo.rows[0]['tipo']=='mail_nuevo_usuario' )
            {
                await client.query(` UPDATE public.email_envios_logs SET estado='ENVIANDO' where id=`+Correo.rows[0]['id']+` `);
                await enviarEmail.mail_nuevo_usuario({
                    nombre:Correo.rows[0]['nombre']
                    , apellido:Correo.rows[0]['datos']
                    , email:Correo.rows[0]['para']
                    , usuario:Correo.rows[0]['datos_adicionales']
                    , telefono:Correo.rows[0]['enlace']
                    , asunto:Correo.rows[0]['asunto']
                    , contrasenia:Correo.rows[0]['texto']
                });
                ActualizarEstadoEnvioCorreo(Correo.rows[0]['id'], EstadoCorreo);
            }
            else if( Correo.rows[0]['tipo']=='mail_notificacion_planificacion_confirmada' )
            {
                await client.query(` UPDATE public.email_envios_logs SET estado='ENVIANDO' where id=`+Correo.rows[0]['id']+` `);
                EstadoCorreo = await enviarEmail.mail_notificacion_planificacion_confirmada({
                    asunto:Correo.rows[0]['asunto'],
                    fecha_descarga:Correo.rows[0]['fecha'],
                    email:Correo.rows[0]['para'],
                    contenedor:Correo.rows[0]['datos'],
                    clientes:JSON.parse(Correo.rows[0]['datos_adicionales'])
                 });
                ActualizarEstadoEnvioCorreo(Correo.rows[0]['id'], EstadoCorreo);
            }
            else if( Correo.rows[0]['tipo']=='mail_notificacion_contenedor_proforma' )
            {
                await client.query(` UPDATE public.email_envios_logs SET estado='ENVIANDO' where id=`+Correo.rows[0]['id']+` `);
                EstadoCorreo = await enviarEmail.mail_notificacion_contenedor_proforma({
                    asunto:Correo.rows[0]['asunto'],
                    texto:Correo.rows[0]['texto'],
                    fecha:Correo.rows[0]['fecha'],
                    servicios:JSON.parse(Correo.rows[0]['datos_adicionales']),
                    emails:JSON.parse(Correo.rows[0]['para']),
                    tracking_id:Correo.rows[0]['tracking_encrypt']
                });
                ActualizarEstadoEnvioCorreo(Correo.rows[0]['id'], EstadoCorreo);
            }
            else if( Correo.rows[0]['tipo']=='mail_notificacion_tarifa' )
            {
                await client.query(` UPDATE public.email_envios_logs SET estado='ENVIANDO' where id=`+Correo.rows[0]['id']+` `);
                EstadoCorreo = await enviarEmail.mail_notificacion_tarifa({
                    asunto:Correo.rows[0]['asunto'],
                    fecha:Correo.rows[0]['fecha'],
                    email:Correo.rows[0]['para'],
                    nombre:Correo.rows[0]['nombre'],
                 });
                ActualizarEstadoEnvioCorreo(Correo.rows[0]['id'], EstadoCorreo);
            }
            else if( Correo.rows[0]['tipo']=='mail_notificacion_question' )
            {
                await client.query(` UPDATE public.email_envios_logs SET estado='ENVIANDO' where id=`+Correo.rows[0]['id']+` `);
                EstadoCorreo = await enviarEmail.mail_notificacion_question({
                    asunto:Correo.rows[0]['asunto'],
                    name:Correo.rows[0]['nombre'],
                    linksQuestion:Correo.rows[0]['enlace'],
                    to:Correo.rows[0]['para'],
                    datos: JSON.parse(Correo.rows[0]['datos']),
                    comercial:JSON.parse(Correo.rows[0]['comercial'])
                });
                ActualizarEstadoEnvioCorreo(Correo.rows[0]['id'], EstadoCorreo);
            }
            else if( Correo.rows[0]['tipo']=='mail_notificacion_6' )
            {
                await client.query(` UPDATE public.email_envios_logs SET estado='ENVIANDO' where id=`+Correo.rows[0]['id']+` `);
                EstadoCorreo = await enviarEmail.mail_notificacion_6({
                    asunto:Correo.rows[0]['asunto'],
                    nombreUsuario:Correo.rows[0]['nombre'],
                    fecha:Correo.rows[0]['fecha'],
                    host:Correo.rows[0]['enlace'],
                    tipo:Correo.rows[0]['tipo_id'],
                    email:Correo.rows[0]['para'],
                    comercial:JSON.parse(Correo.rows[0]['comercial']),
                    tracking_id:Correo.rows[0]['tracking_encrypt']
                });
                ActualizarEstadoEnvioCorreo(Correo.rows[0]['id'], EstadoCorreo);
            }
            else if( Correo.rows[0]['tipo']=='mail_notificacion_1' )
            {
                if( Correo.rows[0]['tipo_id']=='16' )
                {
                    await client.query(` UPDATE public.email_envios_logs SET estado='ENVIANDO' where id=`+Correo.rows[0]['id']+` `);
                    EstadoCorreo = await enviarEmail.mail_notificacion_1({
                        asunto:Correo.rows[0]['asunto'],
                        nombreUsuario:Correo.rows[0]['nombre'],
                        fecha:Correo.rows[0]['fecha'],
                        tipo:Correo.rows[0]['tipo_id'],
                        host:Correo.rows[0]['enlace'],
                        email:Correo.rows[0]['para'],
                        comercial:JSON.parse(Correo.rows[0]['comercial']),
                        tracking_id:Correo.rows[0]['tracking_encrypt']
                    });
                    ActualizarEstadoEnvioCorreo(Correo.rows[0]['id'], EstadoCorreo);
                }
                else if( Correo.rows[0]['tipo_id']=='17' )
                {
                    await client.query(` UPDATE public.email_envios_logs SET estado='ENVIANDO' where id=`+Correo.rows[0]['id']+` `);
                    EstadoCorreo = await enviarEmail.mail_notificacion_1({
                        asunto:Correo.rows[0]['asunto'],
                        nombreUsuario:Correo.rows[0]['nombre'],
                        tipo:Correo.rows[0]['tipo_id'],
                        host:Correo.rows[0]['enlace'],
                        fecha:Correo.rows[0]['fecha'],
                        email:Correo.rows[0]['para'],
                        comercial:JSON.parse(Correo.rows[0]['comercial']),
                        tracking_id:Correo.rows[0]['tracking_encrypt']
                    });
                    ActualizarEstadoEnvioCorreo(Correo.rows[0]['id'], EstadoCorreo);
                }
                else if( Correo.rows[0]['tipo_id']=='18' )
                {
                    await client.query(` UPDATE public.email_envios_logs SET estado='ENVIANDO' where id=`+Correo.rows[0]['id']+` `);
                    EstadoCorreo = await enviarEmail.mail_notificacion_1({
                        asunto:Correo.rows[0]['asunto'],
                        nombreUsuario:Correo.rows[0]['nombre'],
                        tipo:Correo.rows[0]['tipo_id'],
                        host:Correo.rows[0]['enlace'],
                        fecha:Correo.rows[0]['fecha'],
                        email:Correo.rows[0]['para'],
                        comercial:JSON.parse(Correo.rows[0]['comercial']),
                        datosAdicionales:JSON.parse(Correo.rows[0]['datos_adicionales']),
                        tracking_id:Correo.rows[0]['tracking_encrypt']
                    });
                    ActualizarEstadoEnvioCorreo(Correo.rows[0]['id'], EstadoCorreo);
                }
                else if( Correo.rows[0]['tipo_id']=='14' )
                {
                    await client.query(` UPDATE public.email_envios_logs SET estado='ENVIANDO' where id=`+Correo.rows[0]['id']+` `);
                    EstadoCorreo = await enviarEmail.mail_notificacion_1({
                        asunto:Correo.rows[0]['asunto'],
                        nombreUsuario:Correo.rows[0]['nombre'],
                        texto:JSON.parse(Correo.rows[0]['texto']),
                        fecha:Correo.rows[0]['fecha'],
                        email:Correo.rows[0]['para'],
                        comercial:JSON.parse(Correo.rows[0]['comercial']),
                        tipo:Correo.rows[0]['tipo_id'],
                        host:Correo.rows[0]['enlace'],
                        datosAdicionales:JSON.parse(Correo.rows[0]['datos_adicionales']),
                        tracking_id:Correo.rows[0]['tracking_encrypt']
                    });
                    ActualizarEstadoEnvioCorreo(Correo.rows[0]['id'], EstadoCorreo);
                }
                else if( Correo.rows[0]['tipo_id']=='22' )
                {
                    await client.query(` UPDATE public.email_envios_logs SET estado='ENVIANDO' where id=`+Correo.rows[0]['id']+` `);
                    EstadoCorreo = await enviarEmail.mail_notificacion_1({
                        asunto:Correo.rows[0]['asunto'],
                        nombreUsuario:Correo.rows[0]['nombre'],
                        fecha:Correo.rows[0]['fecha'],
                        tipo:Correo.rows[0]['tipo_id'],
                        host:Correo.rows[0]['enlace'],
                        email:Correo.rows[0]['para'],
                        comercial:JSON.parse(Correo.rows[0]['comercial']),
                        tracking_id:Correo.rows[0]['tracking_encrypt']
                    });
                    ActualizarEstadoEnvioCorreo(Correo.rows[0]['id'], EstadoCorreo);
                }
                else if( Correo.rows[0]['tipo_id']=='23' )
                {
                    await client.query(` UPDATE public.email_envios_logs SET estado='ENVIANDO' where id=`+Correo.rows[0]['id']+` `);
                    EstadoCorreo = await enviarEmail.mail_notificacion_1({
                        asunto:Correo.rows[0]['asunto'],
                        nombreUsuario:Correo.rows[0]['nombre'],
                        tipo:Correo.rows[0]['tipo_id'],
                        host:Correo.rows[0]['enlace'],
                        fecha:Correo.rows[0]['fecha'],
                        email:Correo.rows[0]['para'],
                        comercial:JSON.parse(Correo.rows[0]['comercial']),
                        tracking_id:Correo.rows[0]['tracking_encrypt']
                    });
                    ActualizarEstadoEnvioCorreo(Correo.rows[0]['id'], EstadoCorreo);
                }
                else if( Correo.rows[0]['tipo_id']=='19' || Correo.rows[0]['tipo_id']=='20')
                {
                    await client.query(` UPDATE public.email_envios_logs SET estado='ENVIANDO' where id=`+Correo.rows[0]['id']+` `);
                    EstadoCorreo = await enviarEmail.mail_notificacion_1({
                        asunto:Correo.rows[0]['asunto'],
                        nombreUsuario:Correo.rows[0]['nombre'],
                        tipo:Correo.rows[0]['tipo_id'],
                        texto:JSON.parse(Correo.rows[0]['texto']),
                        host:Correo.rows[0]['enlace'],
                        fecha:Correo.rows[0]['fecha'],
                        email:Correo.rows[0]['para'],
                        comercial:JSON.parse(Correo.rows[0]['comercial']),
                        tracking_id:Correo.rows[0]['tracking_encrypt']
                    });
                    ActualizarEstadoEnvioCorreo(Correo.rows[0]['id'], EstadoCorreo);
                }
                else if( Correo.rows[0]['tipo_id']=='99' )
                {
                    await client.query(` UPDATE public.email_envios_logs SET estado='ENVIANDO' where id=`+Correo.rows[0]['id']+` `);
                    EstadoCorreo = await enviarEmail.mail_notificacion_1({
                        asunto:Correo.rows[0]['asunto'],
                        nombreUsuario:Correo.rows[0]['nombre'],
                        fecha:Correo.rows[0]['fecha'],
                        email:Correo.rows[0]['para'],
                        texto:JSON.parse(Correo.rows[0]['texto']),
                        comercial:JSON.parse(Correo.rows[0]['comercial']),
                        tipo:Correo.rows[0]['tipo_id'],
                        host:Correo.rows[0]['enlace'],
                        tracking_id:Correo.rows[0]['tracking_encrypt']
                    });
                    ActualizarEstadoEnvioCorreo(Correo.rows[0]['id'], EstadoCorreo);
                }
                else if( Correo.rows[0]['tipo_id']=='100' )
                {
                    await client.query(` UPDATE public.email_envios_logs SET estado='ENVIANDO' where id=`+Correo.rows[0]['id']+` `);
                    EstadoCorreo = await enviarEmail.mail_notificacion_1({
                        asunto:Correo.rows[0]['asunto'],
                        nombreUsuario:Correo.rows[0]['nombre'],
                        fecha:Correo.rows[0]['fecha'],
                        email:Correo.rows[0]['para'],
                        comercial:JSON.parse(Correo.rows[0]['comercial']),
                        tipo:Correo.rows[0]['tipo_id'],
                        host:Correo.rows[0]['enlace'],
                        tracking_id:Correo.rows[0]['tracking_encrypt']
                    });
                    ActualizarEstadoEnvioCorreo(Correo.rows[0]['id'], EstadoCorreo);
                }
                else
                {
                    
                    if( Correo.rows[0]['adjunto']=='' || Correo.rows[0]['adjunto']==null )
                    {
                        await client.query(` UPDATE public.email_envios_logs SET estado='ENVIANDO' where id=`+Correo.rows[0]['id']+` `);
                        EstadoCorreo = await enviarEmail.mail_notificacion_1({
                            asunto:Correo.rows[0]['asunto'],
                            nombreUsuario:Correo.rows[0]['nombre'],
                            texto:JSON.parse(Correo.rows[0]['texto']),
                            fecha:Correo.rows[0]['fecha'],
                            email:Correo.rows[0]['para'],
                            comercial:JSON.parse(Correo.rows[0]['comercial']),
                            tracking_id:Correo.rows[0]['tracking_encrypt'],
                            host:Correo.rows[0]['enlace'],
                        });
                        
                    }
                    ActualizarEstadoEnvioCorreo(Correo.rows[0]['id'], EstadoCorreo);
                }
            }
            else if( Correo.rows[0]['tipo']=='mail_notificacion_pago' )
            {
                await client.query(` UPDATE public.email_envios_logs SET estado='ENVIANDO' where id=`+Correo.rows[0]['id']+` `);

                EstadoCorreo = await enviarEmail.mail_notificacion_pago({
                    asunto:Correo.rows[0]['asunto'],
                    cliente:Correo.rows[0]['nombre'],
                    datos:JSON.parse(Correo.rows[0]['datos']),
                    datosAdicionales:JSON.parse(Correo.rows[0]['datos_pagos']),
                    fecha:Correo.rows[0]['fecha'],
                    email:Correo.rows[0]['para'],
                    emailcomercial:Correo.rows[0]['email_comercial'],
                    comercial:JSON.parse(Correo.rows[0]['comercial'])
                });
                ActualizarEstadoEnvioCorreo(Correo.rows[0]['id'], EstadoCorreo);
            }
            else if( Correo.rows[0]['tipo']=='mail_notificacion_recepcion' )
            {
                await client.query(` UPDATE public.email_envios_logs SET estado='ENVIANDO' where id=`+Correo.rows[0]['id']+` `);
                EstadoCorreo = await enviarEmail.mail_notificacion_recepcion({
                    asunto:Correo.rows[0]['asunto'],
                    cliente:Correo.rows[0]['nombre'],
                    datos:JSON.parse(Correo.rows[0]['datos']),
                    datosAdicionales:JSON.parse(Correo.rows[0]['datos']),
                    fecha:Correo.rows[0]['fecha'],
                    email:Correo.rows[0]['para'],
                    url:Correo.rows[0]['enlace'],
                    emailcomercial:Correo.rows[0]['email_comercial'],
                    comercial:JSON.parse(Correo.rows[0]['comercial']),
                    tracking_id:Correo.rows[0]['tracking_encrypt']
                });
                ActualizarEstadoEnvioCorreo(Correo.rows[0]['id'], EstadoCorreo);
            }
            else if( Correo.rows[0]['tipo']=='mail_notificacion_retiro_programado' )
            {
                await client.query(` UPDATE public.email_envios_logs SET estado='ENVIANDO' where id=`+Correo.rows[0]['id']+` `);
                EstadoCorreo = await enviarEmail.mail_notificacion_retiro_programado({
                    asunto:Correo.rows[0]['asunto'],
                    //cliente:Correo.rows[0]['nombre'],
                    datos:JSON.parse(Correo.rows[0]['datos']),
                    //datosAdicionales:JSON.parse(Correo.rows[0]['datos_adicionales']),
                    fecha:Correo.rows[0]['fecha'],
                    email:Correo.rows[0]['para'],
                    //url:Correo.rows[0]['enlace'],
                    comercial:JSON.parse(Correo.rows[0]['email_comercial']),
                    //comercial:JSON.parse(Correo.rows[0]['comercial']),
                });
                ActualizarEstadoEnvioCorreo(Correo.rows[0]['id'], EstadoCorreo);
            }
            else if( Correo.rows[0]['tipo']=='mail_notificacion_consolidacion_rapida' )
            {
                await client.query(` UPDATE public.email_envios_logs SET estado='ENVIANDO' where id=`+Correo.rows[0]['id']+` `);
                EstadoCorreo = await enviarEmail.mail_notificacion_consolidacion_rapida({
                    asunto:Correo.rows[0]['asunto'],
                    cliente:Correo.rows[0]['nombre'],
                    datos:JSON.parse(Correo.rows[0]['datos']),
                    datosAdicionales:JSON.parse(Correo.rows[0]['datos_adicionales']),
                    fecha:Correo.rows[0]['fecha'],
                    email:Correo.rows[0]['para'],
                    url:Correo.rows[0]['enlace'],
                    emailcomercial:Correo.rows[0]['email_comercial'],
                    comercial:JSON.parse(Correo.rows[0]['comercial']),
                    tracking_id:null
                });
                ActualizarEstadoEnvioCorreo(Correo.rows[0]['id'], EstadoCorreo);
            }
            /*else if( Correo.rows[0]['tipo']=='mail_notificacion_consolidacion_seleccionable' )
            {
                await client.query(` UPDATE public.email_envios_logs SET estado='ENVIANDO' where id=`+Correo.rows[0]['id']+` `);
                let datosAdicionales=JSON.parse(Correo.rows[0]['datos_adicionales']);
                let str='';let str2='';
                if(datosAdicionales.borrados && datosAdicionales.borrados.length){
                    for(let i=0;i<datosAdicionales.borrados.length;i++){
                        if(i<datosAdicionales.borrados.length-1){
                            str+=datosAdicionales.borrados[i]+' - ';
                        }else if(i==datosAdicionales.borrados.length-1){
                            str+=datosAdicionales.borrados[i];
                        }
                    }
                }

                if(datosAdicionales.modificados && datosAdicionales.modificados.length){
                    for(let i=0;i<datosAdicionales.modificados.length;i++){
                        if(i<datosAdicionales.modificados.length-1){
                            str2+=datosAdicionales.modificados[i]+'-';
                        }else if(i==datosAdicionales.modificados.length-1){
                            str2+=datosAdicionales.modificados[i];
                        }
                    }
                }

                datosAdicionales.deletes=str;
                datosAdicionales.modify='';
                if(str2.length>0){
                    datosAdicionales.modify=await encryptText(str2);
                }
                
                EstadoCorreo = await enviarEmail.mail_notificacion_new_consolidado({
                    asunto:Correo.rows[0]['asunto'],
                    cliente:Correo.rows[0]['nombre'],
                    datos:JSON.parse(Correo.rows[0]['datos']),
                    datosAdicionales:datosAdicionales,
                    fecha:Correo.rows[0]['fecha'],
                    email:Correo.rows[0]['para'],
                    url:Correo.rows[0]['enlace'],
                    emailcomercial:Correo.rows[0]['email_comercial'],
                    comercial:JSON.parse(Correo.rows[0]['comercial']),
                    tracking_id:null,
                    attachments: JSON.parse(Correo.rows[0]['adjunto'])
                });
                ActualizarEstadoEnvioCorreo(Correo.rows[0]['id'], EstadoCorreo);
            }*/
            else if( Correo.rows[0]['tipo']=='mail_notificacion_confirmacion_consolidacion_rapida' )
            {
                console.log('\n\n PROCESANDO mail_notificacion_confirmacion_consolidacion_rapida')
                await client.query(` UPDATE public.email_envios_logs SET estado='ENVIANDO' where id=`+Correo.rows[0]['id']+` `);
                EstadoCorreo = await enviarEmail.mail_notificacion_confirmacion_consolidacion_rapida({
                    asunto:Correo.rows[0]['asunto'],
                    cliente:Correo.rows[0]['nombre'],
                    datos:JSON.parse(Correo.rows[0]['datos']),
                    datosAdicionales:JSON.parse(Correo.rows[0]['datos_adicionales']),
                    fecha:Correo.rows[0]['fecha'],
                    email:Correo.rows[0]['para'],
                    url:Correo.rows[0]['enlace'],
                    emailcomercial:Correo.rows[0]['email_comercial'],
                    comercial:JSON.parse(Correo.rows[0]['comercial']),
                    tracking_id:null
                });
                ActualizarEstadoEnvioCorreo(Correo.rows[0]['id'], EstadoCorreo);
            }
        }

        async function ActualizarEstadoEnvioCorreo(id, estado)
        {
            if(EstadoCorreo==true)
            {
                await client.query(` UPDATE public.email_envios_logs SET estado='ENVIADO' where id=`+id+` `);
            }
            else if(EstadoCorreo==false)
            {
                await client.query(` UPDATE public.email_envios_logs SET estado='ERROR' where id=`+id+` `);
            }
            else
            {
                await client.query(` UPDATE public.email_envios_logs SET estado='INDEFINIDO' where id=`+id+` `);
            }
        }
    }
}

exports.mail_envios_server1 = async (req, resp) => {
    try{
    console.log(".::.");
    console.log(".::.");
    console.log("CONSULTANDO CORREOS ");
    var sche_mail_envios_server1 = require('node-schedule');

    sche_mail_envios_server1.scheduleJob('*/20 * * * * *', () => {
        mail_envios_server1();

    });

    async function mail_envios_server1()
    {
        var Correo = await client.query(`
        SELECT
        *
        FROM
        public.email_envios_logs
        where
        estado='PENDIENTE'
        and id>8900
        order by id
        asc limit 1
        `);

        var email = '-.com'
        function validateEmail(email)
        {
            var re = /\S+@\S+\.\S+/;
            return re.test(email);
        }

        var EstadoCorreo = null;
        console.log(".::.");
        console.log(".::.");
        console.log("\n\n\nVALIDANDO EMAIL "+validateEmail(email));
        console.log(".::.");
        console.log(".::.");
        console.log("\n\n\nCANTIDAD DE CORREOS PENDIENTES "+Correo.rows.length);
        if(Correo.rows.length>0)
        {
            console.log(".::.");
            console.log(".::.");
            console.log("\n\n\nENVIANDO ID "+Correo.rows[0]['id']);
            console.log("\n\n\nENVIANDO ID "+Correo.rows[0]['id']);
            console.log("\n\n\nENVIANDO ID "+Correo.rows[0]['id']);
            console.log("\n\n\nENVIANDO TIPO "+Correo.rows[0]['tipo_id']);
            
            if( Correo.rows[0]['tipo']=='mail_nota_de_cobro' )
            {
                await client.query(` UPDATE public.email_envios_logs SET estado='ENVIANDO' where id=`+Correo.rows[0]['id']+` `);
                await client.query(` UPDATE public.email_envios_logs SET estado='ENVIANDO' where id=`+Correo.rows[0]['id']+` `);
                
                let Lista = await client.query(`

                    SELECT pro.id as nc_id, pro.din, pro.cif, pro.arancel, pro.impto_port, pro.tc, pro.m3, pro.peso_carga, pro.pb, pro.carga, pro.costo, pro.tc2, pro.aforo, pro.isp, pro.cda, pro.almacenaje, pro.ajuste_m_1, ajuste_c_1, pro.ajuste_m_2, pro.ajuste_c_2, pro.ajuste_m_3, pro.ajuste_c_3, pro.pallet, pro.pallet_valor_u, pro.ttvp, pro.twsc, pro.otros, pro.detalle_otro,
                    to_char( eta, 'DD-MM-YYYY') as eta,
                    d.contenedor,
                    d.n_carpeta,
                    CASE
                        WHEN pro.excento IS NULL THEN pro.costo
                        ELSE pro.excento
                    END as excento,
                    CASE
                        WHEN pro.afecto IS NULL THEN 0
                        ELSE pro.afecto
                    END as afecto,
                    CASE
                        WHEN pro.fk_despacho=-1 THEN d.codigo_cliente
                        ELSE c.codigo
                    END as codigo_cliente,
                    CASE
                        WHEN pro.fk_despacho=-1 THEN d.rut
                        ELSE c.rut 
                    END as rut,
                    CASE
                        WHEN pro.fk_despacho=-1 THEN d.razon_social
                        ELSE c."razonSocial" 
                    END as razon_social,
                    CASE
                        WHEN pro.fk_despacho=-1 THEN d.fk_cliente
                        ELSE c.id 
                    END as id_cliente,
                    (SELECT cd.direccion FROM public.clientes_direcciones cd  WHERE cd.fk_cliente=c.id and cd.fk_tipo=1 and cd.estado>=0 order by cd.id limit 1) as direccion_facturacion,
                    (SELECT cd.numero FROM public.clientes_direcciones cd  WHERE cd.fk_cliente=c.id and cd.fk_tipo=1 and cd.estado>=0 order by cd.id limit 1) as direccion_numero_facturacion,
                    (SELECT cm.nombre FROM public.clientes_direcciones cd INNER JOIN public.comunas cm on cm.id=cd.fk_comuna WHERE cd.fk_cliente=c.id and cd.fk_tipo=1 and cd.estado>=0 order by cd.id limit 1) as direccion_comuna,
                    (SELECT r.codigo_maximize FROM public.clientes_direcciones cd  INNER JOIN public.region r on r.id=cd.fk_region WHERE cd.fk_cliente=c.id and cd.fk_tipo=1 and cd.estado>=0 order by cd.id limit 1) as fk_region,
                    (SELECT cm.codigo_maximize FROM public.clientes_direcciones cd INNER JOIN public.comunas cm on cm.id=cd.fk_comuna WHERE cd.fk_cliente=c.id and cd.fk_tipo=1 and cd.estado>=0 order by cd.id limit 1) as fk_comuna,
                    (SELECT cm.fk_ciudad FROM public.clientes_direcciones cd INNER JOIN public.comunas cm on cm.id=cd.fk_comuna WHERE cd.fk_cliente=c.id and cd.fk_tipo=1 and cd.estado>=0 order by cd.id limit 1) as fk_ciudad, 
                    CASE 
                        WHEN d.fk_nave = 70
                            THEN (SELECT n2.nave_nombre FROM naves2 n2 LEFT JOIN naves_eta ne on ne.fk_nave=n2.nave_id LEFT JOIN contenedor_viajes_detalle cvd on cvd.fk_nave_eta=ne.id LEFT JOIN contenedor_tracking ctr on ctr.id=cvd.fk_contenedor_tracking LEFT join contenedor c ON c.id=ctr.fk_contenedor left join despachos d on d.contenedor=c.codigo where c.codigo=d.contenedor and d.eta=ne.eta_fecha limit 1)
                        ELSE 
                            d.nave_nombre
                    END AS nave_nombre,
                    ROUND((coalesce(pro.cif, 0)+coalesce(pro.arancel, 0))*19/100, 2) as calc_iva, 
                    ROUND(((coalesce(pro.cif, 0)+coalesce(pro.arancel, 0))*19/100)+coalesce(pro.arancel, 0)+coalesce(pro.impto_port, 0), 2) as calc_din_usd,
                    ROUND(coalesce(pro.carga, 0)*coalesce(pro.costo, 0), 2) as calc_pv,
                    ROUND(coalesce(pro.carga, 0)*coalesce(pro.costo, 0)+coalesce(pro.pb, 0), 2) as calc_serv_usd,
                    ROUND((((coalesce(pro.cif, 0)+coalesce(pro.arancel, 0))*19/100)+coalesce(pro.arancel, 0)+coalesce(pro.impto_port, 0))*coalesce(pro.tc, 0)) as calc_din_clp,
                    ROUND((coalesce(pro.carga, 0)*coalesce(pro.costo, 0)+coalesce(pro.pb, 0))*coalesce(pro.tc2, 0)) as calc_serv_clp,
                    ROUND((coalesce(pro.carga, 0)*coalesce(pro.costo, 0)+coalesce(pro.pb, 0))*coalesce(pro.tc2, 0)+coalesce(pro.aforo, 0)+coalesce(pro.isp, 0)+coalesce(pro.cda, 0)+coalesce(pro.almacenaje, 0)) as calc_serv_otros,
                    ROUND((coalesce(pro.carga, 0)*coalesce(pro.costo, 0)+coalesce(pro.pb, 0))*coalesce(pro.tc2, 0)+coalesce(pro.ajuste_m_2, 0)+coalesce(pro.aforo, 0)+coalesce(pro.isp, 0)+coalesce(pro.cda, 0)+coalesce(pro.almacenaje, 0) + (((coalesce(pro.cif, 0)+coalesce(pro.arancel, 0))*19/100)+coalesce(pro.arancel, 0)+coalesce(pro.impto_port, 0))*coalesce(pro.tc, 0)+coalesce(pro.ajuste_m_1, 0) + coalesce(pro.pallet, 0)*coalesce(pro.pallet_valor_u, 0)+coalesce(pro.ttvp, 0)+coalesce(pro.twsc, 0)+coalesce(pro.otros, 0)) as calc_total,
                    ROUND(coalesce((SELECT SUM(dtl.debit_amt)
                        FROM public.wsc_envio_asientos_detalles dtl
                        INNER JOIN public.wsc_envio_asientos_cabeceras cbc
                            ON dtl.fk_cabecera=cbc.id
                        WHERE cbc.carpeta=d.n_carpeta and cbc.estado<>'false' and dtl.desc_text='BANCO SANTANDER'), 0)) as MONTO_PAGADO,
                    ROUND((coalesce(pro.carga, 0)*coalesce(pro.costo, 0)+coalesce(pro.pb, 0))*coalesce(pro.tc2, 0)+coalesce(pro.ajuste_m_2, 0)+coalesce(pro.aforo, 0)+coalesce(pro.isp, 0)+coalesce(pro.cda, 0)+coalesce(pro.almacenaje, 0) + (((coalesce(pro.cif, 0)+coalesce(pro.arancel, 0))*19/100)+coalesce(pro.arancel, 0)+coalesce(pro.impto_port, 0))*coalesce(pro.tc, 0)+coalesce(pro.ajuste_m_1, 0) + coalesce(pro.pallet, 0)*coalesce(pro.pallet_valor_u, 0)+coalesce(pro.ttvp, 0)+coalesce(pro.twsc, 0)+coalesce(pro.otros, 0))-ROUND(coalesce((SELECT SUM(dtl.debit_amt)
                                            FROM public.wsc_envio_asientos_detalles dtl
                                            INNER JOIN public.wsc_envio_asientos_cabeceras cbc
                                                ON dtl.fk_cabecera=cbc.id
                                            WHERE cbc.carpeta=d.n_carpeta and cbc.estado<>'false' and dtl.desc_text='BANCO SANTANDER'), 0)) as monto_por_pagar,
                    pro."linkQuestion",
                    (select ct.fk_consolidado
                        from tracking t
                        inner join consolidado_tracking ct on ct.id = t.fk_consolidado_tracking
                        where fk_contenedor = (
                        select id from contenedor c
                        where codigo = d.contenedor limit 1) and fk_cliente = d.fk_cliente order by ct.fk_consolidado desc limit 1) as fk_servicio,
                    (select referencia from gc_propuestas_cabeceras where fk_servicio = (select ct.fk_consolidado
                        from tracking t
                        inner join consolidado_tracking ct on ct.id = t.fk_consolidado_tracking
                        where fk_contenedor = (
                        select id from contenedor c
                        where codigo = d.contenedor limit 1) and fk_cliente = d.fk_cliente order by ct.fk_consolidado desc limit 1) limit 1) as n_referencia
                    FROM public.notas_cobros pro
                    INNER JOIN public.despachos d
                        ON CASE
					   		WHEN pro.fk_despacho = -1 THEN pro.codigo_unificacion=d.codigo_unificacion
					   		ELSE pro.fk_despacho = d.id END
                    LEFT JOIN public.notas_cobros_estados nc
                        ON nc.fk_provision = pro.id
                    LEFT JOIN public.clientes c
                        ON c.id = d.fk_cliente
        
                    WHERE pro.estado <> false AND d.n_carpeta = '`+Correo.rows[0]['datos_adicionales'].split('.')[0]+`'
                    LIMIT 1
                
                `);


                await nota_cobro_wsc_pdf_Ctrl.ActualizarPdfNotaCobro(Lista.rows[0]['nc_id'], Correo.rows[0]['datos_adicionales'].split('.')[0], '');
                
                // await crear_nota_cobro(, Correo.rows[0]['datos']);

                console.log('\n\n');
                console.log('6');
                console.log('\n\n');

                if (Correo.rows[0]['tipo_id'] == 'true') 
                {   
                    if( Correo.rows[0]['enlace']!==null && Correo.rows[0]['enlace'].length>0 )
                    {
                        var enlace = Correo.rows[0]['enlace'];
                    }
                    else
                    {
                        var enlace = '';
                    }

                    EstadoCorreo = await enviarEmail.mail_nota_de_cobro({
                        pdf_file:Correo.rows[0]['datos']
                        , pdf_file_name:Correo.rows[0]['datos_adicionales']
                        , destinatario:Correo.rows[0]['para']
                        , copia_destinatario:Correo.rows[0]['copia']
                        , asunto:Correo.rows[0]['asunto']
                        , correo_sin_din:Correo.rows[0]['tipo_id']
                        , nombre_com:Correo.rows[0]['comercial']
                        , por_pagar:Correo.rows[0]['adjunto']
                        , enlace:enlace
                    });
                } 
                else 
                {
                    if( Correo.rows[0]['enlace'].length>0 )
                    {
                        var enlace = Correo.rows[0]['enlace'];
                    }
                    else
                    {
                        var enlace = '';
                    }

                    await client.query(` UPDATE public.email_envios_logs SET estado='ENVIANDO' where id=`+Correo.rows[0]['id']+` `);
                    EstadoCorreo = await enviarEmail.mail_nota_de_cobro_din({
                        pdf_file:Correo.rows[0]['datos']
                        , pdf_file_name:Correo.rows[0]['datos_adicionales']
                        , din_pdf_file:Correo.rows[0]['nombre']
                        , din_pdf_file_name:Correo.rows[0]['copia_oculta']
                        , destinatario:Correo.rows[0]['para']
                        , copia_destinatario:Correo.rows[0]['copia']
                        , asunto:Correo.rows[0]['asunto']
                        , correo_sin_din:Correo.rows[0]['tipo_id']
                        , data_extra:JSON.parse(Correo.rows[0]['comercial'])
                        , comercial:JSON.parse(Correo.rows[0]['adjunto'])
                        , asunto_reply:Correo.rows[0]['email_comercial']
                        , enlace:enlace
                    });
                }

                if(EstadoCorreo==true)
                {
                    await client.query(` `+Correo.rows[0]['texto'].split('VALUES')[0] + 'VALUES' + Correo.rows[0]['texto'].split('VALUES')[1].split('(').join("('").split(')').join("')").split(',').join("','")+` `);
                }
                ActualizarEstadoEnvioCorreo(Correo.rows[0]['id'], EstadoCorreo);
            }
            else if( Correo.rows[0]['tipo']=='mail_factura_nota_de_cobro' )
            {
                await client.query(` UPDATE public.email_envios_logs SET estado='ENVIANDO' where id=`+Correo.rows[0]['id']+` `);
                let archivos = JSON.parse(Correo.rows[0]['adjunto']);

                let attch = [];
                let flag = true;

                for (value in archivos){
                    if (flag) {
                        attch.push({
                            filename: archivos[value],
                            path: archivos[value+'_path'],
                        })
                        flag = false;
                    } else {
                        flag = true
                    }
                }

                let Lista = await client.query(`

                    SELECT pro.id as nc_id, pro.din, pro.cif, pro.arancel, pro.impto_port, pro.tc, pro.m3, pro.peso_carga, pro.pb, pro.carga, pro.costo, pro.tc2, pro.aforo, pro.isp, pro.cda, pro.almacenaje, pro.ajuste_m_1, ajuste_c_1, pro.ajuste_m_2, pro.ajuste_c_2, pro.ajuste_m_3, pro.ajuste_c_3, pro.pallet, pro.pallet_valor_u, pro.ttvp, pro.twsc, pro.otros, pro.detalle_otro,
                    to_char( eta, 'DD-MM-YYYY') as eta,
                    d.contenedor,
                    d.n_carpeta,
                    CASE
                        WHEN pro.excento IS NULL THEN pro.costo
                        ELSE pro.excento
                    END as excento,
                    CASE
                        WHEN pro.afecto IS NULL THEN 0
                        ELSE pro.afecto
                    END as afecto,
                    CASE
                        WHEN pro.fk_despacho=-1 THEN d.codigo_cliente
                        ELSE c.codigo
                    END as codigo_cliente,
                    CASE
                        WHEN pro.fk_despacho=-1 THEN d.rut
                        ELSE c.rut 
                    END as rut,
                    CASE
                        WHEN pro.fk_despacho=-1 THEN d.razon_social
                        ELSE c."razonSocial" 
                    END as razon_social,
                    CASE
                        WHEN pro.fk_despacho=-1 THEN d.fk_cliente
                        ELSE c.id 
                    END as id_cliente,
                    (SELECT cd.direccion FROM public.clientes_direcciones cd  WHERE cd.fk_cliente=c.id and cd.fk_tipo=1 and cd.estado>=0 order by cd.id limit 1) as direccion_facturacion,
                    (SELECT cd.numero FROM public.clientes_direcciones cd  WHERE cd.fk_cliente=c.id and cd.fk_tipo=1 and cd.estado>=0 order by cd.id limit 1) as direccion_numero_facturacion,
                    (SELECT cm.nombre FROM public.clientes_direcciones cd INNER JOIN public.comunas cm on cm.id=cd.fk_comuna WHERE cd.fk_cliente=c.id and cd.fk_tipo=1 and cd.estado>=0 order by cd.id limit 1) as direccion_comuna,
                    (SELECT r.codigo_maximize FROM public.clientes_direcciones cd  INNER JOIN public.region r on r.id=cd.fk_region WHERE cd.fk_cliente=c.id and cd.fk_tipo=1 and cd.estado>=0 order by cd.id limit 1) as fk_region,
                    (SELECT cm.codigo_maximize FROM public.clientes_direcciones cd INNER JOIN public.comunas cm on cm.id=cd.fk_comuna WHERE cd.fk_cliente=c.id and cd.fk_tipo=1 and cd.estado>=0 order by cd.id limit 1) as fk_comuna,
                    (SELECT cm.fk_ciudad FROM public.clientes_direcciones cd INNER JOIN public.comunas cm on cm.id=cd.fk_comuna WHERE cd.fk_cliente=c.id and cd.fk_tipo=1 and cd.estado>=0 order by cd.id limit 1) as fk_ciudad, 
                    CASE 
                        WHEN d.fk_nave = 70
                            THEN (SELECT n2.nave_nombre FROM naves2 n2 LEFT JOIN naves_eta ne on ne.fk_nave=n2.nave_id LEFT JOIN contenedor_viajes_detalle cvd on cvd.fk_nave_eta=ne.id LEFT JOIN contenedor_tracking ctr on ctr.id=cvd.fk_contenedor_tracking LEFT join contenedor c ON c.id=ctr.fk_contenedor left join despachos d on d.contenedor=c.codigo where c.codigo=d.contenedor and d.eta=ne.eta_fecha limit 1)
                        ELSE 
                            d.nave_nombre
                    END AS nave_nombre,
                    ROUND((coalesce(pro.cif, 0)+coalesce(pro.arancel, 0))*19/100, 2) as calc_iva, 
                    ROUND(((coalesce(pro.cif, 0)+coalesce(pro.arancel, 0))*19/100)+coalesce(pro.arancel, 0)+coalesce(pro.impto_port, 0), 2) as calc_din_usd,
                    ROUND(coalesce(pro.carga, 0)*coalesce(pro.costo, 0), 2) as calc_pv,
                    ROUND(coalesce(pro.carga, 0)*coalesce(pro.costo, 0)+coalesce(pro.pb, 0), 2) as calc_serv_usd,
                    ROUND((((coalesce(pro.cif, 0)+coalesce(pro.arancel, 0))*19/100)+coalesce(pro.arancel, 0)+coalesce(pro.impto_port, 0))*coalesce(pro.tc, 0)) as calc_din_clp,
                    ROUND((coalesce(pro.carga, 0)*coalesce(pro.costo, 0)+coalesce(pro.pb, 0))*coalesce(pro.tc2, 0)) as calc_serv_clp,
                    ROUND((coalesce(pro.carga, 0)*coalesce(pro.costo, 0)+coalesce(pro.pb, 0))*coalesce(pro.tc2, 0)+coalesce(pro.aforo, 0)+coalesce(pro.isp, 0)+coalesce(pro.cda, 0)+coalesce(pro.almacenaje, 0)) as calc_serv_otros,
                    ROUND((coalesce(pro.carga, 0)*coalesce(pro.costo, 0)+coalesce(pro.pb, 0))*coalesce(pro.tc2, 0)+coalesce(pro.ajuste_m_2, 0)+coalesce(pro.aforo, 0)+coalesce(pro.isp, 0)+coalesce(pro.cda, 0)+coalesce(pro.almacenaje, 0) + (((coalesce(pro.cif, 0)+coalesce(pro.arancel, 0))*19/100)+coalesce(pro.arancel, 0)+coalesce(pro.impto_port, 0))*coalesce(pro.tc, 0)+coalesce(pro.ajuste_m_1, 0) + coalesce(pro.pallet, 0)*coalesce(pro.pallet_valor_u, 0)+coalesce(pro.ttvp, 0)+coalesce(pro.twsc, 0)+coalesce(pro.otros, 0)) as calc_total,
                    ROUND(coalesce((SELECT SUM(dtl.debit_amt)
                        FROM public.wsc_envio_asientos_detalles dtl
                        INNER JOIN public.wsc_envio_asientos_cabeceras cbc
                            ON dtl.fk_cabecera=cbc.id
                        WHERE cbc.carpeta=d.n_carpeta and cbc.estado<>'false' and dtl.desc_text='BANCO SANTANDER'), 0)) as MONTO_PAGADO,
                    ROUND((coalesce(pro.carga, 0)*coalesce(pro.costo, 0)+coalesce(pro.pb, 0))*coalesce(pro.tc2, 0)+coalesce(pro.ajuste_m_2, 0)+coalesce(pro.aforo, 0)+coalesce(pro.isp, 0)+coalesce(pro.cda, 0)+coalesce(pro.almacenaje, 0) + (((coalesce(pro.cif, 0)+coalesce(pro.arancel, 0))*19/100)+coalesce(pro.arancel, 0)+coalesce(pro.impto_port, 0))*coalesce(pro.tc, 0)+coalesce(pro.ajuste_m_1, 0) + coalesce(pro.pallet, 0)*coalesce(pro.pallet_valor_u, 0)+coalesce(pro.ttvp, 0)+coalesce(pro.twsc, 0)+coalesce(pro.otros, 0))-ROUND(coalesce((SELECT SUM(dtl.debit_amt)
                                            FROM public.wsc_envio_asientos_detalles dtl
                                            INNER JOIN public.wsc_envio_asientos_cabeceras cbc
                                                ON dtl.fk_cabecera=cbc.id
                                            WHERE cbc.carpeta=d.n_carpeta and cbc.estado<>'false' and dtl.desc_text='BANCO SANTANDER'), 0)) as monto_por_pagar,
                    pro."linkQuestion",
                    (select ct.fk_consolidado
                        from tracking t
                        inner join consolidado_tracking ct on ct.id = t.fk_consolidado_tracking
                        where fk_contenedor = (
                        select id from contenedor c
                        where codigo = d.contenedor limit 1) and fk_cliente = d.fk_cliente order by ct.fk_consolidado desc limit 1) as fk_servicio,
                    (select referencia from gc_propuestas_cabeceras where fk_servicio = (select ct.fk_consolidado
                        from tracking t
                        inner join consolidado_tracking ct on ct.id = t.fk_consolidado_tracking
                        where fk_contenedor = (
                        select id from contenedor c
                        where codigo = d.contenedor limit 1) and fk_cliente = d.fk_cliente order by ct.fk_consolidado desc limit 1) limit 1) as n_referencia
                    FROM public.notas_cobros pro
                    INNER JOIN public.despachos d
                        ON CASE
					   		WHEN pro.fk_despacho = -1 THEN pro.codigo_unificacion=d.codigo_unificacion
					   		ELSE pro.fk_despacho = d.id END
                    LEFT JOIN public.notas_cobros_estados nc
                        ON nc.fk_provision = pro.id
                    LEFT JOIN public.clientes c
                        ON c.id = d.fk_cliente
        
                    WHERE pro.estado <> false AND d.n_carpeta = '`+archivos.nc_file.split('.')[0]+`'
                    LIMIT 1
                
                `);


                await nota_cobro_wsc_pdf_Ctrl.ActualizarPdfNotaCobro(Lista.rows[0]['nc_id'], archivos.nc_file.split('.')[0], '');
                
                //await crear_nota_cobro(archivos.nc_file.split('.')[0], archivos.nc_file_path);

                console.log('\n\n');
                console.log('5');
                console.log('\n\n');

                EstadoCorreo = await enviarEmail.mail_nota_de_cobro_factura({ 
                    asunto:Correo.rows[0]['asunto'],
                    email: 'edo.v81@gmail.com', /*lalo Correo.rows[0]['para'], */
                    copia: 'edo.v81@gmail.com', /*lalo Correo.rows[0]['copia'], */
                    comercial:JSON.parse(Correo.rows[0]['comercial']),
                    data_extra:JSON.parse(Correo.rows[0]['datos_adicionales']),
                    archivos:attch,
                });
                ActualizarEstadoEnvioCorreo(Correo.rows[0]['id'], EstadoCorreo);
            }  
            else if( Correo.rows[0]['tipo']=='mail_notificacion_proceso_documental' )
            {
                await client.query(` UPDATE public.email_envios_logs SET estado='ENVIANDO' where id=`+Correo.rows[0]['id']+` `);
                EstadoCorreo = await enviarEmail.mail_notificacion_proceso_documental({
                    from: 'wscargo@wscargo.cl',
                    to: Correo.rows[0]['para'],
                    cc: Correo.rows[0]['copia'],
                    asunto: Correo.rows[0]['asunto'],
                    RepresentanteLegal: Correo.rows[0]['nombre'],
                    attachment: JSON.parse(Correo.rows[0]['adjunto'])
                });
                ActualizarEstadoEnvioCorreo(Correo.rows[0]['id'], EstadoCorreo);
            }
            else if( Correo.rows[0]['tipo']=='mail_etiquetas_2022_clientes' )
            {
                await client.query(` UPDATE public.email_envios_logs SET estado='ENVIANDO' where id=`+Correo.rows[0]['id']+` `);
                EstadoCorreo = await enviarEmail.mail_etiquetas_2022_clientes({
                    from: 'wscargo@wscargo.cl',
                    to: Correo.rows[0]['para'],
                    cc: Correo.rows[0]['copia'],
                    representante:Correo.rows[0]['nombre'],
                    replyTo: Correo.rows[0]['respondera'],
                    subject: Correo.rows[0]['asunto'],
                    attachments: JSON.parse(Correo.rows[0]['adjunto'])
                });
                ActualizarEstadoEnvioCorreo(Correo.rows[0]['id'], EstadoCorreo);
            }
            else if( Correo.rows[0]['tipo']=='mail_notificacion_etiqueta_cliente' )
            {
                await client.query(` UPDATE public.email_envios_logs SET estado='ENVIANDO' where id=`+Correo.rows[0]['id']+` `);
                EstadoCorreo = await enviarEmail.mail_notificacion_etiqueta_cliente({
                    asunto:Correo.rows[0]['asunto'],
                    fecha:Correo.rows[0]['fecha'],
                    email:Correo.rows[0]['para'],
                    nombre:Correo.rows[0]['nombre'],
                    fk_cliente:Correo.rows[0]['tipo_id'],
                });
                ActualizarEstadoEnvioCorreo(Correo.rows[0]['id'], EstadoCorreo);
            }
            else if( Correo.rows[0]['tipo']=='mail_notificacion_1' )
            {
                if( Correo.rows[0]['tipo_id']=='16' || Correo.rows[0]['tipo_id']=='17' || Correo.rows[0]['tipo_id']=='18' || Correo.rows[0]['tipo_id']=='14'
                || Correo.rows[0]['tipo_id']=='22' || Correo.rows[0]['tipo_id']=='23' || Correo.rows[0]['tipo_id']=='19' || Correo.rows[0]['tipo_id']=='20'
                || Correo.rows[0]['tipo_id']=='99' || Correo.rows[0]['tipo_id']=='100' )
                {
                    
                }
                else
                {
                    if( Correo.rows[0]['adjunto']!='' && Correo.rows[0]['adjunto']!=null )
                    {
                        await client.query(` UPDATE public.email_envios_logs SET estado='ENVIANDO' where id=`+Correo.rows[0]['id']+` `);
                        EstadoCorreo = await enviarEmail.mail_notificacion_1({
                            asunto:Correo.rows[0]['asunto'],
                            nombreUsuario:Correo.rows[0]['nombre'],
                            texto:JSON.parse(Correo.rows[0]['texto']),
                            fecha:Correo.rows[0]['fecha'],
                            email:Correo.rows[0]['para'],
                            attachments:JSON.parse(Correo.rows[0]['adjunto']),
                            comercial:JSON.parse(Correo.rows[0]['comercial']),
                            tracking_id:Correo.rows[0]['tracking_encrypt'],
                            host:Correo.rows[0]['enlace'],
                        });
                        ActualizarEstadoEnvioCorreo(Correo.rows[0]['id'], EstadoCorreo);
                    }
                }
            } else if( Correo.rows[0]['tipo']=='mail_notificacion_consolidacion_seleccionable' )
            {
                await client.query(` UPDATE public.email_envios_logs SET estado='ENVIANDO' where id=`+Correo.rows[0]['id']+` `);
                let datosAdicionales=JSON.parse(Correo.rows[0]['datos_adicionales']);
                let str='';let str2='';
                if(datosAdicionales.borrados && datosAdicionales.borrados.length){
                    for(let i=0;i<datosAdicionales.borrados.length;i++){
                        if(i<datosAdicionales.borrados.length-1){
                            str+=datosAdicionales.borrados[i]+' - ';
                        }else if(i==datosAdicionales.borrados.length-1){
                            str+=datosAdicionales.borrados[i];
                        }
                    }
                }

                if(datosAdicionales.modificados && datosAdicionales.modificados.length){
                    for(let i=0;i<datosAdicionales.modificados.length;i++){
                        if(i<datosAdicionales.modificados.length-1){
                            str2+=datosAdicionales.modificados[i]+'-';
                        }else if(i==datosAdicionales.modificados.length-1){
                            str2+=datosAdicionales.modificados[i];
                        }
                    }
                }

                datosAdicionales.deletes=str;
                datosAdicionales.modify='';
                if(str2.length>0){
                    datosAdicionales.modify=await encryptText(str2);
                }
                
                EstadoCorreo = await enviarEmail.mail_notificacion_new_consolidado({
                    asunto:Correo.rows[0]['asunto'],
                    cliente:Correo.rows[0]['nombre'],
                    datos:JSON.parse(Correo.rows[0]['datos']),
                    datosAdicionales:datosAdicionales,
                    fecha:Correo.rows[0]['fecha'],
                    email:Correo.rows[0]['para'],
                    url:Correo.rows[0]['enlace'],
                    emailcomercial:Correo.rows[0]['email_comercial'],
                    comercial:JSON.parse(Correo.rows[0]['comercial']),
                    tracking_id:null,
                    attachments: JSON.parse(Correo.rows[0]['adjunto'])
                });
                ActualizarEstadoEnvioCorreo(Correo.rows[0]['id'], EstadoCorreo);
            }
            else if( Correo.rows[0]['tipo']=='mail_notificacion_consolidacion_seleccionable' )
            {
                await client.query(` UPDATE public.email_envios_logs SET estado='ENVIANDO' where id=`+Correo.rows[0]['id']+` `);
                let datosAdicionales=JSON.parse(Correo.rows[0]['datos_adicionales']);
                let str='';let str2='';
                if(datosAdicionales.borrados && datosAdicionales.borrados.length){
                    for(let i=0;i<datosAdicionales.borrados.length;i++){
                        if(i<datosAdicionales.borrados.length-1){
                            str+=datosAdicionales.borrados[i]+' - ';
                        }else if(i==datosAdicionales.borrados.length-1){
                            str+=datosAdicionales.borrados[i];
                        }
                    }
                }

                if(datosAdicionales.modificados && datosAdicionales.modificados.length){
                    for(let i=0;i<datosAdicionales.modificados.length;i++){
                        if(i<datosAdicionales.modificados.length-1){
                            str2+=datosAdicionales.modificados[i]+'-';
                        }else if(i==datosAdicionales.modificados.length-1){
                            str2+=datosAdicionales.modificados[i];
                        }
                    }
                }

                datosAdicionales.deletes=str;
                datosAdicionales.modify='';
                if(str2.length>0){
                    datosAdicionales.modify=await encryptText(str2);
                }
                
                EstadoCorreo = await enviarEmail.mail_notificacion_new_consolidado({
                    asunto:Correo.rows[0]['asunto'],
                    cliente:Correo.rows[0]['nombre'],
                    datos:JSON.parse(Correo.rows[0]['datos']),
                    datosAdicionales:datosAdicionales,
                    fecha:Correo.rows[0]['fecha'],
                    email:Correo.rows[0]['para'],
                    url:Correo.rows[0]['enlace'],
                    emailcomercial:Correo.rows[0]['email_comercial'],
                    comercial:JSON.parse(Correo.rows[0]['comercial']),
                    tracking_id:null,
                    attachments: JSON.parse(Correo.rows[0]['adjunto'])
                });
                ActualizarEstadoEnvioCorreo(Correo.rows[0]['id'], EstadoCorreo);
            }
        }

        async function ActualizarEstadoEnvioCorreo(id, estado)
        {
            if(EstadoCorreo==true)
            {
                await client.query(` UPDATE public.email_envios_logs SET estado='ENVIADO' where id=`+id+` `);
            }
            else if(EstadoCorreo==false)
            {
                await client.query(` UPDATE public.email_envios_logs SET estado='ERROR' where id=`+id+` `);
            }
            else
            {
                await client.query(` UPDATE public.email_envios_logs SET estado='INDEFINIDO' where id=`+id+` `);
            }
        }
    }

    }catch(error){
        console.log("ERROR correos :" +error.message);
    }
}

exports.envio_notificacion_32 = async () => {
    var nodeSchedule=require('node-schedule');

    nodeSchedule.scheduleJob('*/60 * * * * *', async () => {

        let today=moment().format('YYYY-MM-DD');
        let results=await client.query(`
        SELECT 
        p.fk_servicio,
        p.referencia,
        p.fk_cliente,
        cl."razonSocial",
        cl."dteEmail"
        FROM public.tracking_entrega te
        INNER JOIN public.consolidado_tracking ct on ct.fk_tracking=te.fk_tracking
        INNER JOIN public.consolidado c on c.id=ct.fk_consolidado
        INNER JOIN public.gc_propuestas_cabeceras p on p.id=c.fk_propuesta
        INNER JOIN public.clientes cl on cl.id=p.fk_cliente
        WHERE 
        te.fecha_programada::date='`+today+`'::date and
        te.estado=true group by p.fk_servicio,p.referencia,p.fk_cliente,cl."razonSocial", cl."dteEmail"
        `);

        try{
            const notificaciones = await getNotificacion32();
            if(notificaciones.length > 0){
                console.log("INICIO PROCESO ENVIO DE NOTIFICACION 32", moment().format('DD-MM-YYYY HH:mm:ss'));
                let index = 0;
                let timer = setInterval( () => {
                    if(index === notificaciones.length){
                        clearInterval(timer);
                        console.log("FINALIZO PROCESO ENVIO DE NOTIFICACION 32", moment().format('DD-MM-YYYY HH:mm:ss'));
                    }else{
                        loopElements(notificaciones[index]);
                        index++;
                    }
                }, 15000);


            }

        }catch(error){
            console.log("ERROR en envio_notificacion_32 :" +error.message);
        }
    });
}

const loopElements = async (element) => {
    console.log("enviando notificacion a "+ element.nombre_corto);
    const update = await updateEstadoNotificacion32(element);
    if(update.command == 'UPDATE')
    {
        await generateAndSendNotiticacion32(element);
    }
}

const getNotificacion32 = async () => {
    try{
        let sql = `select * from notificacion_32 where estado = 'PENDIENTE' AND fecha_entrega < (now() - '1 day'::INTERVAL);`;
        var lista = await client.query(sql);
        if(lista.rows.length > 0){
            return lista.rows;
        }else{
            return [];
        }

      } catch (error) {
          console.log("ERROR en getNotificacion32 :" +error.message);
      }
}

const updateEstadoNotificacion32 = async (notificacion) => {
    try{
        let sql = `UPDATE notificacion_32 SET estado = 'ENVIADA', "updateAt" = NOW() WHERE id = ${notificacion.id} RETURNING * ;`;
        var result = await client.query(sql);
        return result;

      } catch (error) {
          console.log("ERROR en updateEstadoNotificacion32 :" +error.message);
      }
}

const generateAndSendNotiticacion32 = async (notificacion) => {

    const rgxValidarEmail = /^[-\w.%+]{1,64}@(?:[A-Z0-9-]{1,63}\.){1,125}[A-Z]{2,63}$/i;
    if(rgxValidarEmail.test(notificacion.email_cliente)){
        let options = {
            from: 'wscargo@wscargo.cl',
            to: notificacion.email_cliente,
            notificacion:notificacion,
            subject: `WSCargo | Te invitamos a evaluar tu servicio ${notificacion.fk_consolidado} ${notificacion.referencia === null || notificacion.referencia === '' ? '': ` | ${notificacion.referencia}` }  recientemente terminado  ${moment().format('DD-MM-YYYY HH:mm:ss')}.`,
        };
        await enviarEmail.mail_notificacion_32(options);
    }else{
        console.log(`EL EMAIL ${notificacion.email_cliente} DEL CLIENTE ${notificacion.nombre_corto} NO ES UN EMAIL VALIDO.`);
    }
}

exports.envio_notificacion_31 = async () => {
    var nodeSchedule=require('node-schedule');
    nodeSchedule.scheduleJob('00 10 * * *', async () => {
        
        try{
            console.log("INICIO PROCESO ENVIO DE NOTIFICACION 31", moment().format('DD-MM-YYYY HH:mm:ss'));
            const notificaciones = await getNotificacion31();
            if(notificaciones.length > 0){
                let index = 0;
                let timer = setInterval( () => { 
                    if(index === notificaciones.length){
                        clearInterval(timer);
                        console.log("FINALIZO PROCESO ENVIO DE NOTIFICACION 31", moment().format('DD-MM-YYYY HH:mm:ss'));
                    }else{
                        loopElementsNotificacion31(notificaciones[index]);
                        index++;
                    }
                }, 15000); 

                
            }else{
                console.log("FINALIZO PROCESO ENVIO DE NOTIFICACION 31 - NO HABIAN NOTIFICACIONES PENDIENTES", moment().format('DD-MM-YYYY HH:mm:ss'));
            }
          
        }catch(error){
            console.log("ERROR en envio_notificacion_31 :" +error.message);
        }
    });
}  

const loopElementsNotificacion31 = async (element) => {
    console.log("enviando notificacion a "+ element.nombre);
    const update = await updateEstadoNotificacion31(element);
    if(update.command == 'UPDATE'){
        await generateAndSendNotiticacion31(element);
    }
}

const getNotificacion31 = async () => {
    try{
        let sql = `select * from notificacion_31 where estado = 'PENDIENTE' AND "createAt" < (now() - '1 day'::INTERVAL);`;
        var lista = await client.query(sql);
        if(lista.rows.length > 0){
            return lista.rows;
        }else{
            return [];
        }
    
      } catch (error) {
          console.log("ERROR en getNotificacion31 :" +error.message);
      }
}

const updateEstadoNotificacion31 = async (notificacion) => {
    try{
        let sql = `UPDATE notificacion_31 SET estado = 'ENVIADA', "updateAt" = NOW() WHERE id = ${notificacion.id} RETURNING * ;`;
        var result = await client.query(sql);
        return result;
    
      } catch (error) {
          console.log("ERROR en updateEstadoNotificacion31 :" +error.message);
      }
}

const generateAndSendNotiticacion31 = async (notificacion) => {

    const rgxValidarEmail = /^[-\w.%+]{1,64}@(?:[A-Z0-9-]{1,63}\.){1,125}[A-Z]{2,63}$/i;
    if(rgxValidarEmail.test(notificacion.para)){
        let opciones = {
            asunto:notificacion.asunto,
            name:notificacion.nombre,
            linksQuestion:notificacion.enlace,
            to:notificacion.para,
            datos: JSON.parse(notificacion.datos),
            comercial:JSON.parse(notificacion.comercial)
        }
        await enviarEmail.mail_notificacion_question(opciones);
    }else{
        console.log(`EL EMAIL ${notificacion.para} DEL CLIENTE ${notificacion.nombre} NO ES UN EMAIL VALIDO.`);
    }

}
const crear_nota_cobro = async(n_carpeta, filePath) => {
    try {
        var moment = require('moment');
        var currencyFormatter = require('currency-formatter');
        var fs = require('fs');

        let id = n_carpeta; //aca debe llegar el n_carpeta
        let fecha = moment(Date.now()).format("DD-MM-YYYY");

        if (!fs.existsSync(filePath.substring(0, filePath.length-14))) {
            fs.mkdirSync(filePath.substring(0, filePath.length-14), { recursive: true })
        }

        const generateQRCodeImage = async (id, link) => {
            const pathQr = path.resolve('./server/controllers/etiquetas/'+id+'.png');
            const codeQR = link;
            return new Promise((resolve, reject) => {
              QRCode.toFile(pathQr,codeQR, {
                    dark: "#000",
                    light: "#0000",
                }, (err) => {
                  if (err){
                    return reject(err.message);
                  }
                  return resolve(pathQr);
                }
              );
            });
          };

        let Lista = await client.query(`

                    SELECT pro.din, pro.cif, pro.arancel, pro.impto_port, pro.tc, pro.m3, pro.peso_carga, pro.pb, pro.carga, pro.costo, pro.tc2, pro.aforo, pro.isp, pro.cda, pro.almacenaje, pro.ajuste_m_1, ajuste_c_1, pro.ajuste_m_2, pro.ajuste_c_2, pro.ajuste_m_3, pro.ajuste_c_3, pro.pallet, pro.pallet_valor_u, pro.ttvp, pro.twsc, pro.otros, pro.detalle_otro,
                    to_char( eta, 'DD-MM-YYYY') as eta,
                    d.contenedor,
                    d.n_carpeta,
                    CASE
                        WHEN pro.excento IS NULL THEN pro.costo
                        ELSE pro.excento
                    END as excento,
                    CASE
                        WHEN pro.afecto IS NULL THEN 0
                        ELSE pro.afecto
                    END as afecto,
                    CASE
                        WHEN pro.fk_despacho=-1 THEN d.codigo_cliente
                        ELSE c.codigo
                    END as codigo_cliente,
                    CASE
                        WHEN pro.fk_despacho=-1 THEN d.rut
                        ELSE c.rut 
                    END as rut,
                    CASE
                        WHEN pro.fk_despacho=-1 THEN d.razon_social
                        ELSE c."razonSocial" 
                    END as razon_social,
                    CASE
                        WHEN pro.fk_despacho=-1 THEN d.fk_cliente
                        ELSE c.id 
                    END as id_cliente,
                    (SELECT cd.direccion FROM public.clientes_direcciones cd  WHERE cd.fk_cliente=c.id and cd.fk_tipo=1 and cd.estado>=0 order by cd.id limit 1) as direccion_facturacion,
                    (SELECT cd.numero FROM public.clientes_direcciones cd  WHERE cd.fk_cliente=c.id and cd.fk_tipo=1 and cd.estado>=0 order by cd.id limit 1) as direccion_numero_facturacion,
                    (SELECT cm.nombre FROM public.clientes_direcciones cd INNER JOIN public.comunas cm on cm.id=cd.fk_comuna WHERE cd.fk_cliente=c.id and cd.fk_tipo=1 and cd.estado>=0 order by cd.id limit 1) as direccion_comuna,
                    (SELECT r.codigo_maximize FROM public.clientes_direcciones cd  INNER JOIN public.region r on r.id=cd.fk_region WHERE cd.fk_cliente=c.id and cd.fk_tipo=1 and cd.estado>=0 order by cd.id limit 1) as fk_region,
                    (SELECT cm.codigo_maximize FROM public.clientes_direcciones cd INNER JOIN public.comunas cm on cm.id=cd.fk_comuna WHERE cd.fk_cliente=c.id and cd.fk_tipo=1 and cd.estado>=0 order by cd.id limit 1) as fk_comuna,
                    (SELECT cm.fk_ciudad FROM public.clientes_direcciones cd INNER JOIN public.comunas cm on cm.id=cd.fk_comuna WHERE cd.fk_cliente=c.id and cd.fk_tipo=1 and cd.estado>=0 order by cd.id limit 1) as fk_ciudad, 
                    CASE 
                        WHEN d.fk_nave = 70
                            THEN (SELECT n2.nave_nombre FROM naves2 n2 LEFT JOIN naves_eta ne on ne.fk_nave=n2.nave_id LEFT JOIN contenedor_viajes_detalle cvd on cvd.fk_nave_eta=ne.id LEFT JOIN contenedor_tracking ctr on ctr.id=cvd.fk_contenedor_tracking LEFT join contenedor c ON c.id=ctr.fk_contenedor left join despachos d on d.contenedor=c.codigo where c.codigo=d.contenedor and d.eta=ne.eta_fecha limit 1)
                        ELSE 
                            d.nave_nombre
                    END AS nave_nombre,
                    ROUND((coalesce(pro.cif, 0)+coalesce(pro.arancel, 0))*19/100, 2) as calc_iva, 
                    ROUND(((coalesce(pro.cif, 0)+coalesce(pro.arancel, 0))*19/100)+coalesce(pro.arancel, 0)+coalesce(pro.impto_port, 0), 2) as calc_din_usd,
                    ROUND(coalesce(pro.carga, 0)*coalesce(pro.costo, 0), 2) as calc_pv,
                    ROUND(coalesce(pro.carga, 0)*coalesce(pro.costo, 0)+coalesce(pro.pb, 0), 2) as calc_serv_usd,
                    ROUND((((coalesce(pro.cif, 0)+coalesce(pro.arancel, 0))*19/100)+coalesce(pro.arancel, 0)+coalesce(pro.impto_port, 0))*coalesce(pro.tc, 0)) as calc_din_clp,
                    ROUND((coalesce(pro.carga, 0)*coalesce(pro.costo, 0)+coalesce(pro.pb, 0))*coalesce(pro.tc2, 0)) as calc_serv_clp,
                    ROUND((coalesce(pro.carga, 0)*coalesce(pro.costo, 0)+coalesce(pro.pb, 0))*coalesce(pro.tc2, 0)+coalesce(pro.aforo, 0)+coalesce(pro.isp, 0)+coalesce(pro.cda, 0)+coalesce(pro.almacenaje, 0)) as calc_serv_otros,
                    ROUND((coalesce(pro.carga, 0)*coalesce(pro.costo, 0)+coalesce(pro.pb, 0))*coalesce(pro.tc2, 0)+coalesce(pro.ajuste_m_2, 0)+coalesce(pro.aforo, 0)+coalesce(pro.isp, 0)+coalesce(pro.cda, 0)+coalesce(pro.almacenaje, 0) + (((coalesce(pro.cif, 0)+coalesce(pro.arancel, 0))*19/100)+coalesce(pro.arancel, 0)+coalesce(pro.impto_port, 0))*coalesce(pro.tc, 0)+coalesce(pro.ajuste_m_1, 0) + coalesce(pro.pallet, 0)*coalesce(pro.pallet_valor_u, 0)+coalesce(pro.ttvp, 0)+coalesce(pro.twsc, 0)+coalesce(pro.otros, 0)) as calc_total,
                    ROUND(coalesce((SELECT SUM(dtl.debit_amt)
                        FROM public.wsc_envio_asientos_detalles dtl
                        INNER JOIN public.wsc_envio_asientos_cabeceras cbc
                            ON dtl.fk_cabecera=cbc.id
                        WHERE cbc.carpeta=d.n_carpeta and cbc.estado<>'false' and dtl.desc_text='BANCO SANTANDER'), 0)) as MONTO_PAGADO,
                    ROUND((coalesce(pro.carga, 0)*coalesce(pro.costo, 0)+coalesce(pro.pb, 0))*coalesce(pro.tc2, 0)+coalesce(pro.ajuste_m_2, 0)+coalesce(pro.aforo, 0)+coalesce(pro.isp, 0)+coalesce(pro.cda, 0)+coalesce(pro.almacenaje, 0) + (((coalesce(pro.cif, 0)+coalesce(pro.arancel, 0))*19/100)+coalesce(pro.arancel, 0)+coalesce(pro.impto_port, 0))*coalesce(pro.tc, 0)+coalesce(pro.ajuste_m_1, 0) + coalesce(pro.pallet, 0)*coalesce(pro.pallet_valor_u, 0)+coalesce(pro.ttvp, 0)+coalesce(pro.twsc, 0)+coalesce(pro.otros, 0))-ROUND(coalesce((SELECT SUM(dtl.debit_amt)
                                            FROM public.wsc_envio_asientos_detalles dtl
                                            INNER JOIN public.wsc_envio_asientos_cabeceras cbc
                                                ON dtl.fk_cabecera=cbc.id
                                            WHERE cbc.carpeta=d.n_carpeta and cbc.estado<>'false' and dtl.desc_text='BANCO SANTANDER'), 0)) as monto_por_pagar,
                    pro."linkQuestion",
                    (select ct.fk_consolidado
                        from tracking t
                        inner join consolidado_tracking ct on ct.id = t.fk_consolidado_tracking
                        where fk_contenedor = (
                        select id from contenedor c
                        where codigo = d.contenedor limit 1) and fk_cliente = d.fk_cliente order by ct.fk_consolidado desc limit 1) as fk_servicio,
                    (select referencia from gc_propuestas_cabeceras where fk_servicio = (select ct.fk_consolidado
                        from tracking t
                        inner join consolidado_tracking ct on ct.id = t.fk_consolidado_tracking
                        where fk_contenedor = (
                        select id from contenedor c
                        where codigo = d.contenedor limit 1) and fk_cliente = d.fk_cliente order by ct.fk_consolidado desc limit 1) limit 1) as n_referencia
                    FROM public.notas_cobros pro
                    INNER JOIN public.despachos d
                        ON CASE
					   		WHEN pro.fk_despacho = -1 THEN pro.codigo_unificacion=d.codigo_unificacion
					   		ELSE pro.fk_despacho = d.id END
                    LEFT JOIN public.notas_cobros_estados nc
                        ON nc.fk_provision = pro.id
                    LEFT JOIN public.clientes c
                        ON c.id = d.fk_cliente
        
                    WHERE pro.estado <> false AND d.n_carpeta = '${id}'
                    LIMIT 1
                
            `);

        console.log('Creando pdf de ', id);

        let tv = Number(Lista.rows[0].carga) * Number(Lista.rows[0].costo);
        let tv_exento = Number(Lista.rows[0].carga) * Number(Lista.rows[0].excento);
        let tv_afecto = Number(Lista.rows[0].carga) * Number(Lista.rows[0].afecto);

        let iva_afecto = Number((Number(Lista.rows[0].carga) * (Number(Lista.rows[0].afecto)))*0.19).toFixed(0);

        let total_excento = Number((Number(Lista.rows[0].pb) + Number(Lista.rows[0].carga) * Number(Lista.rows[0].excento))).toFixed(0);
        let total_afecto = Number(Lista.rows[0].calc_serv_usd) - Number(total_excento) + Number(iva_afecto);

        let total_servicio_usd = Number(Number(total_excento) + Number(total_afecto)).toFixed(0);
        let total_servicio = Number(Number(Lista.rows[0].tc2)*(Number(total_excento) + Number(total_afecto))).toFixed(0);

        let aforo = Number(Lista.rows[0].aforo);
        let isp = Number(Lista.rows[0].isp);
        let cda = Number(Lista.rows[0].cda);
        let almacenaje = Number(Lista.rows[0].almacenaje);
        let pallets = Number(Lista.rows[0].pallet) * Number(Lista.rows[0].pallet_valor_u);
        let ttvp = Number(Lista.rows[0].ttvp);
        let twsc = Number(Lista.rows[0].twsc);
        let otros = Number(Lista.rows[0].otros);
        let detalle_otro = Lista.rows[0].detalle_otro;
        let ajuste_c_1 = Lista.rows[0].ajuste_c_1;
        let ajuste_c_2 = Lista.rows[0].ajuste_c_2;
        let ajuste_c_3 = Lista.rows[0].ajuste_c_3;
        let ajuste_m_1 = Number(Lista.rows[0].ajuste_m_1);
        let ajuste_m_2 = Number(Lista.rows[0].ajuste_m_2);

        let ajustes = Number(Lista.rows[0].ajuste_m_1) + Number(Lista.rows[0].ajuste_m_2);

        let flag_otros_cobros = (aforo != 0 || isp != 0 || cda != 0 || almacenaje != 0 || pallets != 0 || ttvp != 0 || twsc != 0 || otros != 0 || ajustes != 0) ? true : false;
        let total_otros = aforo + isp + cda + almacenaje + pallets + ttvp + twsc + otros + ajuste_m_2 + ajuste_m_1;

        let total_pro = Number(Number(total_servicio) + Number(Lista.rows[0].calc_din_clp) + Number(total_otros)).toFixed(0);

        let por_pagar = Number(Number(total_pro) - Number(Lista.rows[0].monto_pagado)).toFixed(0) > 0 ? Number(Number(total_pro) - Number(Lista.rows[0].monto_pagado)).toFixed(0) : 0;

        // pdf 
        let pathLogoCabecera = path.resolve("./server/controllers/etiquetas/LogoBlanco.png");
        let pathFont = path.resolve('./server/controllers/fonts/pingfang.ttf');
        let pathLogo = path.resolve("./server/controllers/etiquetas/LogoPequeno.png");
        let pathFontBold = path.resolve('./server/controllers/fonts/pingfang-bold.ttf');

        let pathQr = null;
        console.log('linkQuestion', Lista.rows[0].linkQuestion)
        if (Lista.rows[0].linkQuestion != null) {
            pathQr = await generateQRCodeImage(Lista.rows[0].id_cliente, Lista.rows[0].linkQuestion);
        }

        console.log('\n\n');
        console.log('1');
        console.log('\n\n');
        const pathLocal = filePath;

        var doc = new PDF({ size: 'A4' });

        console.log('\n\n');
        console.log('2');
        console.log('\n\n');

        doc.pipe(fs.createWriteStream(pathLocal));

        // cabecera
        doc
            // data left
            .image(pathLogo, 30, 25, { width: 75, height: 28 })
            .font(pathFontBold)
            .fontSize(8)
            .text('WS Cargo SpA', 115, 25)
            .fontSize(7)
            .text('Rut: 77.166.807-0', 115, 36)
            .text('Dirección: Av Andrés Bello 2299, of 202', 115, 45)
            // data right
            .lineWidth(1.2)
            .rect(450, 25, 120, 32).fillAndStroke('#fff', '#E30613')
            .fill('#E30613').stroke()
            .fontSize(8)
            .font(pathFontBold)
            .text('Despacho - 档案信息', 460, 30, { width: 100 })
            .text('N° ' + Lista.rows[0].n_carpeta, 460, 40);

        // datos del cliente
        doc
            // box (rectangulo)
            .lineWidth(1)
            .rect(30, 80, 540, 50).fillAndStroke('#fff', '#000')
            .fill('#000').stroke()

            // Rectango blanco que va dejado del texto
            .lineWidth(1)
            .rect(40, 75, 108, 10).fillAndStroke('#fff', '#fff')
            .fill('#000').stroke()

            // title del item (CLIENTE - 客户名信息)
            .font(pathFontBold)
            .fontSize(9)
            .text('CLIENTE - 客户名信息', 45, 73)

        doc
            .fontSize(7)
            .font(pathFontBold)
            .text("RUT: ", 45, 90)
            .text("CLIENTE: ", 45, 100)
            .text("DIRECCIÓN: ", 45, 110)
            .text('COD. CLIENTE: ' + Lista.rows[0].id_cliente, 40, 90, {
                width: 515,
                align: 'right'
            })

            .fill('#5a5a5a').stroke()
            .font(pathFontBold)
            .text(Lista.rows[0].rut, 65, 90, { lineBreak: false })
            .text(Lista.rows[0].razon_social, 80, 100, { lineBreak: false })
            .text(Lista.rows[0].direccion_facturacion + ' ' + Lista.rows[0].direccion_numero_facturacion + ' ' + Lista.rows[0].direccion_comuna, 90, 110, { lineBreak: false })

        // Detalle de carga
        doc
            .lineWidth(1)
            .rect(30, 141, 540, 55).fillAndStroke('#fff', '#000')
            .fill('#000').stroke()

            .lineWidth(1)
            .rect(40, 140, 135, 10).fillAndStroke('#fff', '#fff')
            .fill('#000').stroke()

            .font(pathFontBold)
            .fontSize(9)
            .text('DETALLE CARGA - 货柜信息', 45, 135)

            // col data m3
            // peso - fecha
            .fontSize(7)
            .font(pathFontBold)
            .text("M3: ", 45, 153)
            .text("PESO (Kg): ", 45, 163)
            .text("FECHA - 日期: ", 45, 173)

            .fill('#5a5a5a').stroke()
            .font(pathFontBold)
            .text(Lista.rows[0].m3, 62, 153)
            .text(Lista.rows[0].peso_carga, 87, 163)
            .text(fecha, 98, 173)

            // col data conrenedor,
            // nave, eta
            .fill('#000').stroke()
            .fontSize(7)
            .font(pathFontBold)
            .text("CONTENEDOR - 柜号: ", 235, 153)
            .text("NAVE - 船名: ", 235, 163)
            .text("ETA - 到港日期: ", 235, 173)

            .fill('#5a5a5a').stroke()
            .font(pathFontBold)
            .text(Lista.rows[0].contenedor, 313, 153)
            .text(Lista.rows[0].nave_nombre, 283, 163)
            .text(Lista.rows[0].eta, 292, 173)

            // col nuemro interno,
            // numero din
            .fill('#000').stroke()
            .font(pathFontBold)
            .text("N° DIN - 进口单号:", 435, 153)
            .text("N° SERVICIO:", 435, 163)
            .text("N° REFERENCIA:", 435, 173)
            .fill('#5a5a5a').stroke()
            .font(pathFontBold)
            .text(Lista.rows[0].din, 500, 153, {
                width: 100,
                align: 'left'
            })
            .text(Lista.rows[0].fk_servicio, 484, 163, {
                width: 100,
                align: 'left'
            })
            .text(Lista.rows[0].n_referencia, 492, 173, {
                width: 100,
                align: 'left'
            })

        // DETALLE ITEM GASTOS
        // CABECERA GASTOS E IMPUESTOS
        doc
            .lineWidth(1)
            .rect(30, 210, 540, 590).fillAndStroke('#fff', '#000')
            .fill('#000').stroke()
            .lineWidth(1)
            .rect(40, 205, 170, 10).fillAndStroke('#fff', '#fff')
            .fill('#000').stroke()
            .font(pathFontBold)
            .fontSize(9)
            .text('GASTOS E IMPUESTOS - 税金与开支', 45, 205)

        // SUB CABECERA BG GRIS IMPUESTO ADUADEROS
        doc
            .lineWidth(1)
            .rect(45, 227, 510, 14).fillAndStroke('#F4F4F7', '#F4F4F7')
            .fill('#000').stroke()
            .fontSize(8)
            .font(pathFontBold)
            .text("Total Impuestos Aduaneros - 海关税金总额", 50, 228)
            .text(currencyFormatter.format(Number(Lista.rows[0].calc_din_clp), { code: 'CLP' }), 70, 228, {
                width: 480,
                align: 'right'
            });

        // DATA TOTAL
        doc
            // DATA LEFT
            .fontSize(7)
            .fill('#5a5a5a').stroke()
            .font(pathFontBold)
            .text("TOTAL CIF USD - 总出口成本（美金)", 50, 248)
            // BORDER
            .lineWidth(0.5)
            .strokeColor("#aaaaaa")
            .moveTo(50, 260)
            .lineTo(550, 260)
            .stroke()
            .text("ARANCEL USD - 海关关税（美金)", 50, 262)
            // BORDER
            .lineWidth(0.5)
            .strokeColor("#aaaaaa")
            .moveTo(50, 274)
            .lineTo(550, 274)
            .stroke()
            .text("IVA USD - IVA (美金）", 50, 275)
            // BORDER
            .lineWidth(0.5)
            .strokeColor("#aaaaaa")
            .moveTo(50, 287)
            .lineTo(550, 287)
            .stroke()
            .text("IMPTO. PORTUARIO USD -- 港口税（美金)", 50, 288)
            // BORDER
            .lineWidth(0.5)
            .strokeColor("#aaaaaa")
            .moveTo(50, 300)
            .lineTo(550, 300)
            .stroke()
            .fill('#000').stroke()
            .font(pathFontBold)
            .text('TOTAL USD - 总金额（美金）', 50, 302)
            // BORDER
            .lineWidth(0.5)
            .strokeColor("#aaaaaa")
            .moveTo(50, 314)
            .lineTo(550, 314)
            .stroke()
            .text('T/C - 海关月汇率', 50, 316)

            // DATA RIGHT VALORES
            .text(currencyFormatter.format(Number(Lista.rows[0].cif), { code: 'USD' }), 100, 248, {
                width: 400,
                align: 'right'
            })
            .text(currencyFormatter.format(Number(Lista.rows[0].arancel), { code: 'USD' }), 100, 262, {
                width: 400,
                align: 'right'
            })
            .text(currencyFormatter.format(Number(Lista.rows[0].calc_iva), { code: 'USD' }), 100, 275, {
                width: 400,
                align: 'right'
            })
            .text(currencyFormatter.format(Number(Lista.rows[0].impto_port), { code: 'USD' }), 100, 288, {
                width: 400,
                align: 'right'
            })
            .text(currencyFormatter.format(Number(Lista.rows[0].calc_din_usd), { code: 'USD' }), 100, 302, {
                width: 400,
                align: 'right'
            })
            .text(currencyFormatter.format(Number(Lista.rows[0].tc), { code: 'USD' }), 100, 316, {
                width: 400,
                align: 'right'
            })

        // DATA TOTAL GASTOS - 杂费项目
        doc
            // SUB CABECERA BG GRIS TOTAL GASTOS
            .lineWidth(1)
            .rect(45, 335, 510, 14).fillAndStroke('#F4F4F7', '#F4F4F7')
            .fill('#000').stroke()
            .fontSize(7)
            .text("TOTAL GASTOS - 杂费项目", 50, 337)
            .text(currencyFormatter.format(total_servicio, { code: 'CLP' }), 70, 337, {
                width: 480,
                align: 'right'
            })
        // DATA LEFT
        doc
            .fontSize(7)
            .fill('#5a5a5a').stroke()
            .font(pathFontBold)
            .text("EXENTO (不加税金额)", 280, 355)
            .text("AFECTO (需加税金额)", 365, 355)
            .text("TOTAL (总金额)", 100, 355, {
                width: 400,
                align: 'right'
            })
            .text("(A) PRECIO BASE (SI APLICA) - 手续费（美元）[USD]", 50, 365)
            // border
            .lineWidth(0.5)
            .strokeColor("#aaaaaa")
            .moveTo(50, 376)
            .lineTo(550, 376)
            .stroke()
            .text("(B) UNIDADES A COBRAR - 立方 [m3]", 50, 378)
            // border
            .lineWidth(0.5)
            .strokeColor("#aaaaaa")
            .moveTo(50, 390)
            .lineTo(550, 390)
            .stroke()
            .text("(C) USD/UNIDAD - 每立方费用（美元/立方)", 50, 392)
            // border
            .lineWidth(0.5)
            .strokeColor("#aaaaaa")
            .moveTo(50, 404)
            .lineTo(550, 404)
            .stroke()
            .text("(D) = (B)*(C) TOTAL VARIABLE - 总立方费用（美元)", 50, 406)
            // border
            .lineWidth(0.5)
            .strokeColor("#aaaaaa")
            .moveTo(50, 418)
            .lineTo(550, 418)
            .stroke()
            .text("IVA", 50, 419)
            // border
            .lineWidth(0.5)
            .strokeColor("#aaaaaa")
            .moveTo(50, 430)
            .lineTo(550, 430)
            .stroke()

            // total servicios
            .fill('#000').stroke()
            .font(pathFontBold)
            .text("(E) = (A)+(D) TOTAL SERVICIO WS CARGO - WS Cargo 总服务费", 50, 432)
            // border
            .lineWidth(0.5)
            .strokeColor("#aaaaaa")
            .moveTo(50, 445)
            .lineTo(550, 445)
            .stroke()
            .text('T/C - 美元兑披索汇率', 50, 447)
            .fill('#5a5a5a').stroke()
            .font(pathFontBold)
            // DATA VALORES EXENTO
            .text("", 280, 365,)
            .text("", 280, 378,)
            .text(currencyFormatter.format(Number(Lista.rows[0].excento), { code: 'USD' }), 280, 392,)
            .text(currencyFormatter.format(Number(tv_exento), { code: 'USD' }), 280, 406,)
            .text("", 280, 419,)

            // DATA VALORES AFECTO
            .text("", 365, 365,)
            .text("", 365, 378,)
            .text(currencyFormatter.format(Number(Lista.rows[0].afecto), { code: 'USD' }), 365, 392,)
            .text(currencyFormatter.format(Number(tv_afecto), { code: 'USD' }), 365, 406,)
            .text(currencyFormatter.format(Number(iva_afecto), { code: 'USD' }), 365, 419,)

            // DATA VALORES TOTAL
            .text(currencyFormatter.format(Number(Lista.rows[0].pb), { code: 'USD' }), 100, 365, {
                width: 400,
                align: 'right'
            })

            .text(Number(Lista.rows[0].carga).toFixed(2), 100, 378, {
                width: 400,
                align: 'right'
            })
            .text(currencyFormatter.format(Number(Lista.rows[0].costo), { code: 'USD' }), 100, 392, {
                width: 400,
                align: 'right'
            })
            .text(currencyFormatter.format(Number(tv), { code: 'USD' }), 100, 406, {
                width: 400,
                align: 'right'
            })
            .text(currencyFormatter.format(Number(iva_afecto), { code: 'USD' }), 100, 419, {
                width: 400,
                align: 'right'
            })

            // DATA VALORES TOTALES GASTOS
            .fill('#000').stroke()
            .font(pathFontBold)
            .text(currencyFormatter.format(Number(total_excento), { code: 'USD' }), 280, 432,)
            .text(currencyFormatter.format(Number(total_afecto), { code: 'USD' }), 365, 432,)
            .text(currencyFormatter.format(total_servicio_usd, { code: 'USD' }), 100, 432, {
                width: 400,
                align: 'right'
            })
            // T/C
            .text(currencyFormatter.format(Number(Lista.rows[0].tc2), { code: 'USD' }), 100, 447, {
                width: 400,
                align: 'right'
            })
        console.log('\n\n');
        console.log('3');
        console.log('\n\n');
        /// SUB CABECERA BG GRIS OTROS COBROS
        let h_base_otros_cobros = 488;

        if (flag_otros_cobros) {

            doc
            .lineWidth(1.5)
            .rect(45, 467, 510, 14).fillAndStroke('#F4F4F7', '#F4F4F7')
            .fill('#000').stroke()
            .fontSize(7)
            .text("OTROS COBROS - 其他费用", 50, 469)
            .text(currencyFormatter.format(total_otros, { code: 'CLP' }), 70, 469, {
                width: 480,
                align: 'right'
            })
            .fontSize(7)
            .fill('#5a5a5a').stroke()
            .font(pathFontBold)
            if (aforo != 0) {
                // item col left
                doc
                .fill('#5a5a5a').stroke()
                .font(pathFontBold)
                .text("AFORO - 验柜费", 50, h_base_otros_cobros)
                // border
                .lineWidth(0.5)
                .strokeColor("#aaaaaa")
                .moveTo(50, 500)
                .lineTo(550, 500)
                .stroke()
                .fill('#000').stroke()
                .font(pathFontBold)
                .text(currencyFormatter.format(Number(aforo), { code: 'CLP' }), 100, h_base_otros_cobros, {
                    width: 400,
                    align: 'right'
                })
                h_base_otros_cobros+=14;
            }
            if (isp != 0) {
            // item col left
                doc
                .fill('#5a5a5a').stroke()
                .font(pathFontBold)
                .text("ISP - 医疗用品操作费", 50, h_base_otros_cobros)
                // border
                .lineWidth(0.5)
                .strokeColor("#aaaaaa")
                .moveTo(50, (h_base_otros_cobros+12))
                .lineTo(550, (h_base_otros_cobros+12))
                .stroke()
                .fill('#000').stroke()
                .font(pathFontBold)
                .text(currencyFormatter.format(Number(isp), { code: 'CLP' }), 100, h_base_otros_cobros, {
                    width: 400,
                    align: 'right'
                })
                h_base_otros_cobros+=14;
            }
            if (cda != 0) {
                // item col left
                doc
                .fill('#5a5a5a').stroke()
                .font(pathFontBold)
                .text("CDA - 危险品操作费", 50, h_base_otros_cobros)
                // border
                .lineWidth(0.5)
                .strokeColor("#aaaaaa")
                .moveTo(50, (h_base_otros_cobros+12))
                .lineTo(550, (h_base_otros_cobros+12))
                .stroke()
                .fill('#000').stroke()
                .font(pathFontBold)
                .text(currencyFormatter.format(Number(cda), { code: 'CLP' }), 100, h_base_otros_cobros, {
                    width: 400,
                    align: 'right'
                })
                h_base_otros_cobros+=13;
            }
            if (almacenaje != 0) {
                // item col left
                doc
                .fill('#5a5a5a').stroke()
                .font(pathFontBold)
                .text("ALMACENAJE - 仓储费", 50, h_base_otros_cobros)
                // border
                .lineWidth(0.5)
                .strokeColor("#aaaaaa")
                .moveTo(50, (h_base_otros_cobros+12))
                .lineTo(550, (h_base_otros_cobros+12))
                .stroke()
                .fill('#000').stroke()
                .font(pathFontBold)
                .text(currencyFormatter.format(Number(almacenaje), { code: 'CLP' }), 100, h_base_otros_cobros, {
                    width: 400,
                    align: 'right'
                })
                h_base_otros_cobros+=13;
            }
            if (pallets != 0) {
                // item col left
                doc
                .fill('#5a5a5a').stroke()
                .font(pathFontBold)
                .text("PALLET - 栈板", 50, h_base_otros_cobros)
                // border
                .lineWidth(0.5)
                .strokeColor("#aaaaaa")
                .moveTo(50, (h_base_otros_cobros+11))
                .lineTo(550, (h_base_otros_cobros+11))
                .stroke()
                .fill('#000').stroke()
                .font(pathFontBold)
                .text(currencyFormatter.format(Number(pallets), { code: 'CLP' }), 100, h_base_otros_cobros, {
                    width: 400,
                    align: 'right'
                })
                h_base_otros_cobros+=13;
            }
            if (ttvp != 0) {
                // item col left
                doc
                .fill('#5a5a5a').stroke()
                .font(pathFontBold)
                .text("TRANSPORTES TVP - TVP 货运费", 50, h_base_otros_cobros)
                // border
                .lineWidth(0.5)
                .strokeColor("#aaaaaa")
                .moveTo(50, (h_base_otros_cobros+11))
                .lineTo(550, (h_base_otros_cobros+11))
                .stroke()
                .fill('#000').stroke()
                .font(pathFontBold)
                .text(currencyFormatter.format(Number(ttvp), { code: 'CLP' }), 100, h_base_otros_cobros, {
                    width: 400,
                    align: 'right'
                })
                h_base_otros_cobros+=13;
            }
            if (twsc != 0) {
                // item col left
                doc
                .fill('#5a5a5a').stroke()
                .font(pathFontBold)
                .text("TRANSPORTES WSC - WSC 货运费", 50, h_base_otros_cobros)
                // border
                .lineWidth(0.5)
                .strokeColor("#aaaaaa")
                .moveTo(50, (h_base_otros_cobros+11))
                .lineTo(550, (h_base_otros_cobros+11))
                .stroke()
                .fill('#000').stroke()
                .font(pathFontBold)
                .text(currencyFormatter.format(Number(twsc), { code: 'CLP' }), 100, h_base_otros_cobros, {
                    width: 400,
                    align: 'right'
                })
                h_base_otros_cobros+=12;
            }
            if (ajustes != 0) {
                // item col left
                doc
                .fill('#5a5a5a').stroke()
                .font(pathFontBold)
                .text("AJUSTES - 调整", 50, h_base_otros_cobros)
                // border
                .lineWidth(0.5)
                .strokeColor("#aaaaaa")
                .moveTo(50, (h_base_otros_cobros+11))
                .lineTo(550, (h_base_otros_cobros+11))
                .stroke()
                .fill('#000').stroke()
                .font(pathFontBold)
                .text(currencyFormatter.format(Number(ajustes), { code: 'CLP' }), 100, h_base_otros_cobros, {
                    width: 400,
                    align: 'right'
                })
                h_base_otros_cobros+=12;
            }
            if (otros != 0) {
                // item col left
                doc
                .fill('#5a5a5a').stroke()
                .font(pathFontBold)
                .text("OTROS - 其它费用", 50, h_base_otros_cobros)
                // border
                .lineWidth(0.5)
                .strokeColor("#aaaaaa")
                .moveTo(50, (h_base_otros_cobros+12))
                .lineTo(550, (h_base_otros_cobros+12))
                .stroke()
                .text("DETALLE OTRO - 其他明细", 50, (h_base_otros_cobros+13))
                .fill('#000').stroke()
                .font(pathFontBold)
                .text(currencyFormatter.format(Number(otros), { code: 'CLP' }), 100, h_base_otros_cobros, {
                    width: 400,
                    align: 'right'
                })
                .text(detalle_otro, 100, (h_base_otros_cobros+13), {
                    width: 400,
                    align: 'right'
                })
            }
            // valor col right

        }
        // DATA VALORES FINALES
        doc
            // DATA TOTAL PROVISIÓN 
            .lineWidth(1.5)
            .rect(45, 623, 510, 13).fillAndStroke('#ffd690', '#ffd690)')
            .fill('#000').stroke()
            .fontSize(7)
            .text("TOTAL PROVISIÓN - 总需收金额", 50, 625)
            .text(currencyFormatter.format(total_pro, { code: 'CLP' }), 70, 625, {
                width: 480,
                align: 'right'
            })

            // DATA MONTO PAGADO
            .text("MONTO PAGADO - 已付金额", 50, 640)
            .text(currencyFormatter.format(Number(Lista.rows[0].monto_pagado), { code: 'CLP' }), 70, 640, {
                width: 480,
                align: 'right'
            })
            // DATA SALDO POR PAGAR
            .lineWidth(1)
            .rect(45, 653, 510, 13).fillAndStroke('#F9B233', '#F9B233')
            .fill('#000').stroke()
            .fontSize(7)
            .text("SALDO POR PAGAR - 应付金额", 50, 655)
            .text(currencyFormatter.format(por_pagar, { code: 'CLP' }), 70, 655, {
                width: 480,
                align: 'right'
            })
        // datos de transferencia
        // doc.addPage();
        doc
            .fontSize(9)
            .fill('#000').stroke()
            .font(pathFontBold)
            .text("DEPOSITAR O TRANSFERIR A: (账户资料)", 50, 675)
            // item col left
            .fontSize(7)
            .text("Banco: Santander", 50, 690)
            .text("N° Cuenta: 75853041", 50, 700)
            .text("Razón Social:: WS Cargo SpA", 50, 710)
            .text("Rut: 77.166.807-0", 50, 720)
            .text("Correo: pagos@wscargo.cl", 50, 730)
            .text("Asunto: " + Lista.rows[0].n_carpeta + ' – Pago ' + Lista.rows[0].id_cliente + ' ' + Lista.rows[0].codigo_cliente, 50, 740)

            // DATA CODIGO QR
            // PARA REALIZAR ENCUENTA A ZHONO O CRM
        pathQr != null &&
            doc
                .lineWidth(1)
                .rect(477, 680, 79, 97).fillAndStroke('#fff', '#000')
                .fill('#000').stroke()
        // CONDICION PARA MOSTAR QR
        pathQr != null &&
            doc.image(pathQr, 480, 683, { width: 72, height: 72 })
                .fontSize(7)
                .text("Escanea y evalúa", 455, 750, {
                    width: 120,
                    align: 'center'
                })
                .text("tu servicio aquí", 455, 759, {
                    width: 120,
                    align: 'center'
                })
        doc.end();

        console.log('\n\n');
        console.log('4');
        console.log('\n\n');
        //doc.pipe(res);

        // console.log(pathLocal)

        //await promisePdf(filePath, doc, options);
        //console.log(pathLocal);
        //res.download('public/files/notas_de_cobro/'+contenedor+'/'+Lista.rows[0].n_carpeta+'.pdf')
        // res.download(pathLocal)
        return true
    } catch (error) {
        console.log("ERROR " + error);
    }
}
