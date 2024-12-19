const enviarEmail = require('../../handlers/email');
const cryptoJs = require('crypto-js');
const client = require('../config/db.client');
const moment=require('moment');
const fs = require('fs');
const path = require('path');

const clientExp = require('../config/db.client.experienciadigital');
const QRCode = require("qrcode");
const PDF = require('pdfkit');//Importando la libreria de PDFkit
const { verify } = require('crypto');
const funcionesCompartidasCtrl = require('./funcionesCompartidasCtrl.js');
const PDFDocument = require('pdfkit');
const axios = require('axios');
//const ftpConfig = require('../config/ftp');
//const jsftp = require("jsftp");
const { json } = require('body-parser');

const encryptText = async (texto) => {
    //const secret = 'd6F3Efeq';
    const SECRET = process.env.SECRET;
    let hash = cryptoJs.AES.encrypt(texto.toString(), SECRET).toString();
    hash = new Buffer.from(hash).toString("base64");
    //let hash = Math.floor((Math.random() * 9999999999999)+100000000000).toString().slice(0,12)+texto;
    return hash;
}

exports.mail_envios_server2 = async (req, resp) => {
    console.log("\n.::.");
    console.log("\n.::.");
    console.log("\nCONSULTANDO CORREOS 2 ");
    var sche_mail_envios_server2 = require('node-schedule');

    sche_mail_envios_server2.scheduleJob('*/15 * * * * *', () => {
        console.log("\nENTRO EN EL CRON JOB ");
        mail_envios_server2();

    });

    async function mail_envios_server2()
    {
            var QueryGetPendientes = `
            SELECT
            *
            , 
            case when tipo='mail_notificacion_pago' then coalesce(datos_adicionales,'{}')::jsonb
            else '{}' end as datos_pagos
            FROM
            public.email_envios_logs
            where
            estado='PENDIENTE'
            and id>=90034
            and coalesce(intentos,0)<=50
            and (
                tipo='mail_nuevo_usuario' 
                or tipo='mail_notificacion_planificacion_confirmada' 
                or tipo='mail_notificacion_contenedor_proforma' 
                or tipo='mail_notificacion_tarifa' 
                or tipo='mail_notificacion_question' 
                or tipo='mail_notificacion_6' 
                or tipo='mail_notificacion_1' 
                or tipo='mail_notificacion_pago' 
                or tipo='mail_notificacion_recepcion' 
                or tipo='mail_notificacion_retiro_programado' 
                or tipo='mail_notificacion_consolidacion_rapida' 
            )
            order by id
            asc limit 1
            `;

        var Correo = await client.query(QueryGetPendientes);
        var email = '-.com'
        function validateEmail(email)
        {
            var re = /\S+@\S+\.\S+/;
            return re.test(email);
        }

        var EstadoCorreo = null;
        
        if(Correo.rows.length>0)
        {
            console.log("\ntipo "+Correo.rows[0]['tipo']);
            console.log("\ntipo_id "+Correo.rows[0]['tipo_id']);
            console.log("\n.::.");
            console.log("\n.::.");
            console.log("\n\n\n\nENVIANDO ID "+Correo.rows[0]['id']+' INTENTOS '+Correo.rows[0]['intentos']);
            console.log("\n\n\n\nENVIANDO TIPO "+Correo.rows[0]['tipo_id']);
            
            var Intentos= Number(Correo.rows[0]['intentos'])+1;
            await client.query(` UPDATE public.email_envios_logs SET intentos=`+Intentos+` where id=`+Correo.rows[0]['id']+` `);

            if( Correo.rows[0]['tipo']=='mail_nuevo_usuario' )
            {
                console.log("\n\n\n\nINGRESO A mail_nuevo_usuario");
                var Intentos= Number(Correo.rows[0]['intentos'])+1;
                await client.query(` UPDATE public.email_envios_logs SET estado='ENVIANDO', intentos=`+Intentos+` where id=`+Correo.rows[0]['id']+` `);
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
            else if( Correo.rows[0]['tipo']=='mail_notificacion_link_tracking' )
            {
                    var Intentos= Number(Correo.rows[0]['intentos'])+1;
                    await client.query(` UPDATE public.email_envios_logs SET estado='ENVIANDO', intentos=`+Intentos+` where id=`+Correo.rows[0]['id']+` `);
                    EstadoCorreo = await enviarEmail.mail_notificacion_link_tracking({
                        asunto:Correo.rows[0]['asunto'],
                        nombre:Correo.rows[0]['nombre'],
                        fecha:Correo.rows[0]['fecha'],
                        email:Correo.rows[0]['para'],
                        comercial:JSON.parse(Correo.rows[0]['comercial']),
                        datos_adicionales:JSON.parse(Correo.rows[0]['datos_adicionales']),
                     });
                    ActualizarEstadoEnvioCorreo(Correo.rows[0]['id'], EstadoCorreo);
            }
            else if( Correo.rows[0]['tipo']=='mail_notificacion_planificacion_confirmada' )
            {
                console.log("\n\n\n\nINGRESO A mail_notificacion_planificacion_confirmada");
                var Intentos= Number(Correo.rows[0]['intentos'])+1;
                await client.query(` UPDATE public.email_envios_logs SET estado='ENVIANDO', intentos=`+Intentos+` where id=`+Correo.rows[0]['id']+` `);
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
                console.log("\n\n\n\nINGRESO A mail_notificacion_contenedor_proforma");
                var Intentos= Number(Correo.rows[0]['intentos'])+1;
                await client.query(` UPDATE public.email_envios_logs SET estado='ENVIANDO', intentos=`+Intentos+` where id=`+Correo.rows[0]['id']+` `);
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
                console.log("\n\n\n\nINGRESO A mail_notificacion_tarifa");
                var Intentos= Number(Correo.rows[0]['intentos'])+1;
                await client.query(` UPDATE public.email_envios_logs SET estado='ENVIANDO', intentos=`+Intentos+` where id=`+Correo.rows[0]['id']+` `);
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
                console.log("\n\n\n\nINGRESO A mail_notificacion_question");
                var Intentos= Number(Correo.rows[0]['intentos'])+1;
                await client.query(` UPDATE public.email_envios_logs SET estado='ENVIANDO', intentos=`+Intentos+` where id=`+Correo.rows[0]['id']+` `);
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
                console.log("\n\n\n\nINGRESO A mail_notificacion_6");
                var Intentos= Number(Correo.rows[0]['intentos'])+1;
                await client.query(` UPDATE public.email_envios_logs SET estado='ENVIANDO', intentos=`+Intentos+` where id=`+Correo.rows[0]['id']+` `);
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
                console.log("\n\n\n\nINGRESO A mail_notificacion_1");
                if( Correo.rows[0]['tipo_id']=='16' )
                {
                    console.log("\n\n\n\nINGRESO A 16");
                    var Intentos= Number(Correo.rows[0]['intentos'])+1;
                await client.query(` UPDATE public.email_envios_logs SET estado='ENVIANDO', intentos=`+Intentos+` where id=`+Correo.rows[0]['id']+` `);
                    EstadoCorreo = await enviarEmail.mail_notificacion_1({
                        asunto:Correo.rows[0]['asunto'],
                        nombreUsuario:Correo.rows[0]['nombre'],
                        fecha:Correo.rows[0]['fecha'],
                        tipo:Correo.rows[0]['tipo_id'],
                        host:Correo.rows[0]['enlace'],
                        email:Correo.rows[0]['para'],
                        comercial:JSON.parse(Correo.rows[0]['comercial']),
                        tracking_id:Correo.rows[0]['tracking_encrypt'],
                        datos_adicionales:JSON.parse(Correo.rows[0]['datos_adicionales']),
                        timeline:Correo.rows[0]['timeline']
                    });
                    ActualizarEstadoEnvioCorreo(Correo.rows[0]['id'], EstadoCorreo);
                }
                else if( Correo.rows[0]['tipo_id']=='17' )
                {
                    console.log("\n\n\n\nINGRESO A 17");
                    var Intentos= Number(Correo.rows[0]['intentos'])+1;
                await client.query(` UPDATE public.email_envios_logs SET estado='ENVIANDO', intentos=`+Intentos+` where id=`+Correo.rows[0]['id']+` `);
                    EstadoCorreo = await enviarEmail.mail_notificacion_1({
                        asunto:Correo.rows[0]['asunto'],
                        nombreUsuario:Correo.rows[0]['nombre'],
                        tipo:Correo.rows[0]['tipo_id'],
                        host:Correo.rows[0]['enlace'],
                        fecha:Correo.rows[0]['fecha'],
                        email:Correo.rows[0]['para'],
                        comercial:JSON.parse(Correo.rows[0]['comercial']),
                        tracking_id:Correo.rows[0]['tracking_encrypt'],
                        timeline:Correo.rows[0]['timeline']
                    });
                    ActualizarEstadoEnvioCorreo(Correo.rows[0]['id'], EstadoCorreo);
                }
                else if( Correo.rows[0]['tipo_id']=='18' )
                {
                    console.log("\n\n\n\nINGRESO A 18");
                    var Intentos= Number(Correo.rows[0]['intentos'])+1;
                await client.query(` UPDATE public.email_envios_logs SET estado='ENVIANDO', intentos=`+Intentos+` where id=`+Correo.rows[0]['id']+` `);
                    EstadoCorreo = await enviarEmail.mail_notificacion_1({
                        asunto:Correo.rows[0]['asunto'],
                        nombreUsuario:Correo.rows[0]['nombre'],
                        tipo:Correo.rows[0]['tipo_id'],
                        host:Correo.rows[0]['enlace'],
                        fecha:Correo.rows[0]['fecha'],
                        email:Correo.rows[0]['para'],
                        comercial:JSON.parse(Correo.rows[0]['comercial']),
                        datosAdicionales:JSON.parse(Correo.rows[0]['datos_adicionales']),
                        tracking_id:Correo.rows[0]['tracking_encrypt'],
                        timeline:Correo.rows[0]['timeline']
                    });
                    ActualizarEstadoEnvioCorreo(Correo.rows[0]['id'], EstadoCorreo);
                }
                else if( Correo.rows[0]['tipo_id']=='14' )
                {
                    console.log("\n\n\n\nINGRESO A 14");
                    var Intentos= Number(Correo.rows[0]['intentos'])+1;
                await client.query(` UPDATE public.email_envios_logs SET estado='ENVIANDO', intentos=`+Intentos+` where id=`+Correo.rows[0]['id']+` `);
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
                        tracking_id:Correo.rows[0]['tracking_encrypt'],
                        timeline:Correo.rows[0]['timeline']
                    });
                    ActualizarEstadoEnvioCorreo(Correo.rows[0]['id'], EstadoCorreo);
                }
                else if( Correo.rows[0]['tipo_id']=='22' )
                {
                    console.log("\n\n\n\nINGRESO A 22");
                    var Intentos= Number(Correo.rows[0]['intentos'])+1;
                await client.query(` UPDATE public.email_envios_logs SET estado='ENVIANDO', intentos=`+Intentos+` where id=`+Correo.rows[0]['id']+` `);
                    EstadoCorreo = await enviarEmail.mail_notificacion_1({
                        asunto:Correo.rows[0]['asunto'],
                        nombreUsuario:Correo.rows[0]['nombre'],
                        fecha:Correo.rows[0]['fecha'],
                        tipo:Correo.rows[0]['tipo_id'],
                        host:Correo.rows[0]['enlace'],
                        email:Correo.rows[0]['para'],
                        comercial:JSON.parse(Correo.rows[0]['comercial']),
                        tracking_id:Correo.rows[0]['tracking_encrypt'],
                        timeline:Correo.rows[0]['timeline']
                    });
                    ActualizarEstadoEnvioCorreo(Correo.rows[0]['id'], EstadoCorreo);
                }
                else if( Correo.rows[0]['tipo_id']=='23' )
                {
                    console.log("\n\n\n\nINGRESO A 23");
                    var Intentos= Number(Correo.rows[0]['intentos'])+1;
                await client.query(` UPDATE public.email_envios_logs SET estado='ENVIANDO', intentos=`+Intentos+` where id=`+Correo.rows[0]['id']+` `);
                    EstadoCorreo = await enviarEmail.mail_notificacion_1({
                        asunto:Correo.rows[0]['asunto'],
                        nombreUsuario:Correo.rows[0]['nombre'],
                        tipo:Correo.rows[0]['tipo_id'],
                        host:Correo.rows[0]['enlace'],
                        fecha:Correo.rows[0]['fecha'],
                        email:Correo.rows[0]['para'],
                        comercial:JSON.parse(Correo.rows[0]['comercial']),
                        tracking_id:Correo.rows[0]['tracking_encrypt'],
                        timeline:Correo.rows[0]['timeline']
                    });
                    ActualizarEstadoEnvioCorreo(Correo.rows[0]['id'], EstadoCorreo);
                }
                else if( Correo.rows[0]['tipo_id']=='19' || Correo.rows[0]['tipo_id']=='20')
                {
                    console.log("\n\n\n\nINGRESO A 19 || 20");
                    var Intentos= Number(Correo.rows[0]['intentos'])+1;
                await client.query(` UPDATE public.email_envios_logs SET estado='ENVIANDO', intentos=`+Intentos+` where id=`+Correo.rows[0]['id']+` `);
                    EstadoCorreo = await enviarEmail.mail_notificacion_1({
                        asunto:Correo.rows[0]['asunto'],
                        nombreUsuario:Correo.rows[0]['nombre'],
                        tipo:Correo.rows[0]['tipo_id'],
                        texto:JSON.parse(Correo.rows[0]['texto']),
                        host:Correo.rows[0]['enlace'],
                        fecha:Correo.rows[0]['fecha'],
                        email:Correo.rows[0]['para'],
                        comercial:JSON.parse(Correo.rows[0]['comercial']),
                        tracking_id:Correo.rows[0]['tracking_encrypt'],
                        timeline:Correo.rows[0]['timeline']
                    });
                    ActualizarEstadoEnvioCorreo(Correo.rows[0]['id'], EstadoCorreo);
                }
                else if( Correo.rows[0]['tipo_id']=='99' )
                {
                    console.log("\n\n\n\nINGRESO A 99");
                    var Intentos= Number(Correo.rows[0]['intentos'])+1;
                await client.query(` UPDATE public.email_envios_logs SET estado='ENVIANDO', intentos=`+Intentos+` where id=`+Correo.rows[0]['id']+` `);
                    EstadoCorreo = await enviarEmail.mail_notificacion_1({
                        asunto:Correo.rows[0]['asunto'],
                        nombreUsuario:Correo.rows[0]['nombre'],
                        fecha:Correo.rows[0]['fecha'],
                        email:Correo.rows[0]['para'],
                        texto:JSON.parse(Correo.rows[0]['texto']),
                        comercial:JSON.parse(Correo.rows[0]['comercial']),
                        tipo:Correo.rows[0]['tipo_id'],
                        host:Correo.rows[0]['enlace'],
                        tracking_id:Correo.rows[0]['tracking_encrypt'],
                        timeline:Correo.rows[0]['timeline']
                    });
                    ActualizarEstadoEnvioCorreo(Correo.rows[0]['id'], EstadoCorreo);
                }
                else if( Correo.rows[0]['tipo_id']=='100' )
                {
                    console.log("\n\n\n\nINGRESO A 100");
                    var Intentos= Number(Correo.rows[0]['intentos'])+1;
                await client.query(` UPDATE public.email_envios_logs SET estado='ENVIANDO', intentos=`+Intentos+` where id=`+Correo.rows[0]['id']+` `);
                    EstadoCorreo = await enviarEmail.mail_notificacion_1({
                        asunto:Correo.rows[0]['asunto'],
                        nombreUsuario:Correo.rows[0]['nombre'],
                        fecha:Correo.rows[0]['fecha'],
                        email:Correo.rows[0]['para'],
                        comercial:JSON.parse(Correo.rows[0]['comercial']),
                        tipo:Correo.rows[0]['tipo_id'],
                        host:Correo.rows[0]['enlace'],
                        tracking_id:Correo.rows[0]['tracking_encrypt'],
                        timeline:Correo.rows[0]['timeline']
                    });
                    ActualizarEstadoEnvioCorreo(Correo.rows[0]['id'], EstadoCorreo);
                }
                else
                {
                    if( Correo.rows[0]['tipo_id']=='999' || Correo.rows[0]['tipo_id']=='1000' || Correo.rows[0]['tipo_id']==999 || Correo.rows[0]['tipo_id']==1000)
                    {
                        console.log("\n\n\n\nINGRESO A PASO 2");
                        console.log('\nPASO 2');
                    }
                    else
                    {
                        if( Correo.rows[0]['adjunto']=='' || Correo.rows[0]['adjunto']==null )
                        {
                            console.log("\n\n\n\nINGRESO A PASO 2 ADJUNTO NULL");
                            var Intentos= Number(Correo.rows[0]['intentos'])+1;
                await client.query(` UPDATE public.email_envios_logs SET estado='ENVIANDO', intentos=`+Intentos+` where id=`+Correo.rows[0]['id']+` `);
                            EstadoCorreo = await enviarEmail.mail_notificacion_1({
                                asunto:Correo.rows[0]['asunto'],
                                nombreUsuario:Correo.rows[0]['nombre'],
                                texto:JSON.parse(Correo.rows[0]['texto']),
                                fecha:Correo.rows[0]['fecha'],
                                email:Correo.rows[0]['para'],
                                comercial:JSON.parse(Correo.rows[0]['comercial']),
                                tracking_id:Correo.rows[0]['tracking_encrypt'],
                                host:Correo.rows[0]['enlace'],
                                timeline:Correo.rows[0]['timeline']
                            });
                            
                        }
                        ActualizarEstadoEnvioCorreo(Correo.rows[0]['id'], EstadoCorreo);
                        }
                    }
            }
            else if( Correo.rows[0]['tipo']=='mail_notificacion_pago' )
            {
                console.log("\n\n\n\nINGRESO A mail_notificacion_pago");
                var Intentos= Number(Correo.rows[0]['intentos'])+1;
                await client.query(` UPDATE public.email_envios_logs SET estado='ENVIANDO', intentos=`+Intentos+` where id=`+Correo.rows[0]['id']+` `);

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
                console.log("\n\n\n\nINGRESO A mail_notificacion_recepcion");
                var Intentos= Number(Correo.rows[0]['intentos'])+1;
                await client.query(` UPDATE public.email_envios_logs SET estado='ENVIANDO', intentos=`+Intentos+` where id=`+Correo.rows[0]['id']+` `);
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
                console.log("\n\n\n\nINGRESO A mail_notificacion_retiro_programado");
                var Intentos= Number(Correo.rows[0]['intentos'])+1;
                await client.query(` UPDATE public.email_envios_logs SET estado='ENVIANDO', intentos=`+Intentos+` where id=`+Correo.rows[0]['id']+` `);
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
                console.log("\n\n\n\nINGRESO A mail_notificacion_consolidacion_rapida");
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

exports.ProcesarExcelGatillos = async (req, res) => {
    console.log("\n.::.");
    console.log("\n.::.");
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

            console.log('\n.::.');
            console.log('\n.::.');
            console.log('\n.::.');
            console.log('\nInsertando el reporte ');
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

            console.log('\n.::.');
            console.log('\n.::.');
            console.log('\n.::.');
            console.log('\nCapturando el porte');
            var Reporte = await client.query(`
            SELECT DISTINCT t.id as tracking_id,
    coalesce(s.id_proveedor,'') as id_proveedor
    , coalesce(s.nombre_proveedor,'') as nombre_proveedor

    , case when length(s.fecha_creacion_proveedor)>0 and s.fecha_creacion_proveedor!='' and s.fecha_creacion_proveedor is not null and s.fecha_creacion_proveedor!='#¡REF!' then
    coalesce(to_char(to_date(s.fecha_creacion_proveedor, 'DD/MM/YYYY'), 'DD/MM/YYYY'),'') else
    '' end as fecha_creacion_proveedor

    , coalesce(s.id_cliente,'') as id_cliente
    , coalesce(s.razon_social_cliente,'') as razon_social_cliente
    , coalesce(s.ejecutivo,'') as ejecutivo_comercial
    ,CASE
      WHEN c.fk_ejecutivocuenta IS NOT NULL THEN
      coalesce(CONCAT(u.nombre,' ',u.apellidos),'')
      ELSE
      '' 
      END as ejecutivo_cuenta
    , coalesce(s.bultos_esperados::text,'') as bultos_esperados
    , REPLACE(coalesce(s.m3_esperados::text,''), '.', ',') as m3_esperados
    , REPLACE(coalesce(s.m3_recibidos::text,''), '.', ',') as m3_recibidos
    , coalesce(s.peso_esperado::text,'') as peso_esperado
    , coalesce(s.bultos_recepcionados::text,'') as bultos_recepcionados
    , coalesce(s.bodega_recepcion,'') as bodega_recepcion

    , case when s.fecha_ultima_carga_documentos='OK' then s.fecha_ultima_carga_documentos
    when length(s.fecha_ultima_carga_documentos)>0 and s.fecha_ultima_carga_documentos!='' and s.fecha_ultima_carga_documentos is not null and s.fecha_ultima_carga_documentos!='#¡REF!' then
    coalesce(to_char(to_date(s.fecha_ultima_carga_documentos, 'DD/MM/YYYY'), 'DD/MM/YYYY'),'') else

    case when s.fecha_cierre_consolidado_comercial='OK' then s.fecha_cierre_consolidado_comercial
    when length(s.fecha_cierre_consolidado_comercial)>0 and s.fecha_cierre_consolidado_comercial!='' and s.fecha_cierre_consolidado_comercial is not null and s.fecha_cierre_consolidado_comercial!='#¡REF!' then
    coalesce(to_char(to_date(s.fecha_cierre_consolidado_comercial, 'DD/MM/YYYY'), 'DD/MM/YYYY'),'') else
    '' end

    end as fecha_ultima_carga_documentos

    , coalesce(s.fecha_aprobacion_documentos,'') as fecha_aprobacion_documentos

    , case when s.fecha_ultima_recepcion='OK' then s.fecha_ultima_recepcion
    when length(s.fecha_ultima_recepcion)>0 and s.fecha_ultima_recepcion!='' and s.fecha_ultima_recepcion is not null and s.fecha_ultima_recepcion!='#¡REF!' then
    coalesce(to_char(to_date(s.fecha_ultima_recepcion, 'YYYY/MM/DD'), 'DD/MM/YYYY'),'') else
    '' end as fecha_ultima_recepcion_registro_1

    , case when s.fecha_de_creacion_del_consolidado='OK' then s.fecha_de_creacion_del_consolidado
    when length(s.fecha_de_creacion_del_consolidado)>0 and s.fecha_de_creacion_del_consolidado!='' and s.fecha_de_creacion_del_consolidado is not null and s.fecha_de_creacion_del_consolidado!='#¡REF!' then
    coalesce(to_char(to_date(s.fecha_de_creacion_del_consolidado, 'DD/MM/YYYY'), 'DD/MM/YYYY'),'') else
    '' end as fecha_de_creacion_del_consolidado

    , case when s.fecha_cierre_consolidado_comercial='OK' then s.fecha_cierre_consolidado_comercial
    when length(s.fecha_cierre_consolidado_comercial)>0 and s.fecha_cierre_consolidado_comercial!='' and s.fecha_cierre_consolidado_comercial is not null and s.fecha_cierre_consolidado_comercial!='#¡REF!' then
    coalesce(to_char(to_date(s.fecha_cierre_consolidado_comercial, 'DD/MM/YYYY'), 'DD/MM/YYYY'),'') else
    '' end as fecha_cierre_consolidado_comercial

    , coalesce(s.id_consolidado_comercial,'') as id_consolidado_comercial
    , coalesce(s.proforma_id,'') as proforma_id

    , case when s.fecha_consolidado_contenedor='OK' then s.fecha_consolidado_contenedor
    when length(s.fecha_consolidado_contenedor)>0 and s.fecha_consolidado_contenedor!='' and s.fecha_consolidado_contenedor is not null and s.fecha_consolidado_contenedor!='#¡REF!' then
    coalesce(to_char(to_date(s.fecha_consolidado_contenedor, 'DD/MM/YYYY'), 'DD/MM/YYYY'),'') else
    '' end as fecha_consolidado_contenedor

    , case when s.fecha_ingreso_datos_contenedor_nave_eta='OK' then s.fecha_ingreso_datos_contenedor_nave_eta
    when length(s.fecha_ingreso_datos_contenedor_nave_eta)>0 and s.fecha_ingreso_datos_contenedor_nave_eta!='' and s.fecha_ingreso_datos_contenedor_nave_eta is not null and s.fecha_ingreso_datos_contenedor_nave_eta!='#¡REF!' then
    coalesce(to_char(to_date(s.fecha_ingreso_datos_contenedor_nave_eta, 'YYYY/MM/DD'), 'DD/MM/YYYY'),'') else
    '' end as fecha_ingreso_datos_contenedor_nave_eta

    , coalesce(s.n_contenedor,'') as n_contenedor
    , coalesce(s.despacho_id,'') as despacho_id
    , coalesce(s.nombre_nave,'') as nombre_nave

    , case when s.etd_nave_asignada='OK' then s.etd_nave_asignada
    when length(s.etd_nave_asignada)>0 and s.etd_nave_asignada!='' and s.etd_nave_asignada is not null and s.etd_nave_asignada!='#¡REF!' then
    coalesce(to_char(to_date(s.etd_nave_asignada, 'DD/MM/YYYY'), 'DD/MM/YYYY'),'') else
    '' end as etd_nave_asignada

    , case when s.fecha_nueva_etd_o_eta='OK' then s.fecha_nueva_etd_o_eta
    when length(s.fecha_nueva_etd_o_eta)>0 and s.fecha_nueva_etd_o_eta!='' and s.fecha_nueva_etd_o_eta is not null and s.fecha_nueva_etd_o_eta!='#¡REF!' then
    coalesce(to_char(to_date(s.fecha_nueva_etd_o_eta, 'DD/MM/YYYY'), 'DD/MM/YYYY'),'') else
    '' end as fecha_nueva_etd_o_eta

    , case when s.eta='OK' then s.eta
    when length(s.eta)>0 and s.eta!='' and s.eta is not null then
    coalesce(to_char(to_date(s.eta, 'DD/MM/YYYY'), 'DD/MM/YYYY'),'') else
    '' end as eta

    , coalesce(s.n_carpeta,'') as n_carpeta

	, coalesce(s.fecha_retiro_puerto,'') as fecha_retiro_puerto
    , coalesce(s.hora_retiro,'') as hora_retiro

    , case when s.fecha_desconsolidacion_pudahuel='OK' then s.fecha_desconsolidacion_pudahuel
    when length(s.fecha_desconsolidacion_pudahuel)>0 and s.fecha_desconsolidacion_pudahuel!='' and s.fecha_desconsolidacion_pudahuel is not null and s.fecha_desconsolidacion_pudahuel!='#¡REF!'then 
    coalesce(to_char(to_date(s.fecha_desconsolidacion_pudahuel, 'DD/MM/YYYY'), 'DD/MM/YYYY'),'') else
    '' end as fecha_desconsolidacion_pudahuel

    ,case when t.fk_proforma is not null then
	coalesce(to_char(cp.fecha_desconsolidado, 'DD/MM/YYYY'),'') else
    '' end as fecha_listo_entrega 
	
 	,case when t.fk_proforma is not null then
	coalesce(to_char(cp.fecha_desconsolidado, 'HH24:mm'),'') else
    '' end as hora_listo_entrega 
	
    , coalesce(s.hora_desconsolidacion,'') as hora_desconsolidacion
    , coalesce(s.estado_finanzas,'') as estado_finanzas

    , case when s.fecha_de_pago='OK' then s.fecha_de_pago
    when length(s.fecha_de_pago)>0 and s.fecha_de_pago!='' and s.fecha_de_pago is not null and s.fecha_de_pago!='#¡REF!' then
    coalesce(to_char(to_date(s.fecha_de_pago, 'DD/MM/YYYY'), 'DD/MM/YYYY'),'') else
    '' end as fecha_de_pago_registro_1

    , case when s.fecha_ingreso_direccion='OK' then s.fecha_ingreso_direccion
    when length(s.fecha_ingreso_direccion)>0 and s.fecha_ingreso_direccion!='' and s.fecha_ingreso_direccion is not null and s.fecha_ingreso_direccion!='#¡REF!' then
    coalesce(to_char(to_date(s.fecha_ingreso_direccion, 'DD/MM/YYYY'), 'DD/MM/YYYY'),'') else
    '' end as fecha_ingreso_direccion

    , case when s.fecha_entrega_retiro='OK' then s.fecha_entrega_retiro
    when length(s.fecha_entrega_retiro)>0 and s.fecha_entrega_retiro!='' and s.fecha_entrega_retiro is not null and s.fecha_entrega_retiro!='#¡REF!' then
    coalesce(to_char(to_date(s.fecha_entrega_retiro, 'DD/MM/YYYY'), 'DD/MM/YYYY'),'') else
    '' end as fecha_entrega
    , coalesce(s.chofer,'') as chofer
    , cnt.dias_libres

    , coalesce(s.fecha_creacion_cliente,'') as fecha_creacion_cliente
    , case when tdr.fk_registro_direccion is not null then 
	concat(dir.direccion,' ',dir.numero,', ',comunas.nombre,', ',region.nombre) else '' end 
	as fk_direccion_completa
	,case when tdr.fk_registro_direccion is not null then comunas.nombre else '' end as fk_comuna_nombre
    ,region.id as fk_region
	,case when tdr.fk_registro_direccion is not null then region.nombre else '' end as fk_region_nombre
	/*,case when te.fecha_programada is not null then coalesce(to_char(te.fecha_programada, 'DD/MM/YYYY'),'') else
    '' end as fecha_programada */
	,(SELECT te.fecha_programada FROM tracking_entrega te where te.fk_tracking=t.id and te.estado=true order by te.id desc limit 1) as fecha_programada
	,case when tdr.fecha is not null then coalesce(to_char(tdr.fecha, 'DD/MM/YYYY'),'') else
    '' end as fecha_solicitud_despacho 
	,CASE 
                    WHEN tdr.fk_bodega is not null then 'RETIRO' 
                    WHEN tdr.empresa_ext_retiro is not null then 'RETIRA TRANS.EXTERNO' 
                    WHEN tdr.empresa_ext_despacho is not null then 'DESPACHO TRANS.EXTERNO' 
                    WHEN tdr.fk_registro_direccion is null and tdr.fk_bodega is null and tdr.empresa_ext_retiro is null and tdr.empresa_ext_despacho is null then 'SIN ESPECIFICAR'
                    WHEN tdr.fk_registro_direccion is not null and dir.fk_region!=12 and dir.fk_region is not null then 'REVISAR DESPACHO GRATUITO NO INCLUIDO'
                    WHEN tdr.fk_registro_direccion is not null and dir.fk_region=12 and dir.fk_region is not null and dir.fk_comuna in(49,50,51,53,57,59,61,62,64,65,66,69,71,72,76,82,88,91) then 'REVISAR DESPACHO GRATUITO NO INCLUIDO'
                    WHEN tdr.fk_registro_direccion is not null and dir.fk_region=12 and dir.fk_region is not null and dir.fk_comuna not in(49,50,51,53,57,59,61,62,64,65,66,69,71,72,76,82,88,91) then 'DESPACHO GRATIS INCLUIDO'
                    ELSE 'SIN ESPECIFICAR' END tipo_entrega
	/*,CASE WHEN te.estado_entrega=1 then 'ENTREGADO' WHEN te.estado_entrega=2 then 'PARCIAL' ELSE '' END as estado_entrega*/
	,CASE WHEN (SELECT te.estado_entrega FROM tracking_entrega te where te.fk_tracking=t.id and te.estado=true order by te.id desc limit 1)=1 then 'ENTREGADO'
	WHEN (SELECT te.estado_entrega FROM tracking_entrega te where te.fk_tracking=t.id and te.estado=true order by te.id desc limit 1)=1 then 'PARCIAL'
	ELSE '' END estado_entrega
    ,case when t.fk_proforma is not null then
	coalesce(to_char(cp.fecha_salida_puerto, 'DD/MM/YYYY'),'') else
    '' end as fecha_real_etd
    ,case when t.fk_proforma is not null then
	coalesce(to_char(cp.fecha_retiro_puerto, 'DD/MM/YYYY'),'') else
    '' end as fecha_real_eta 
	,case when t.fk_proforma is not null then
	coalesce(to_char(cp.fecha_prog_aforo, 'DD/MM/YYYY'),'') else
    '' end as fecha_publicacion_aforo 
	,case when t.fk_proforma is not null then
	coalesce(to_char(cp.fecha_real_aforo, 'DD/MM/YYYY'),'') else
    '' end as fecha_real_aforo 
	,case when t.fk_proforma is not null then
	coalesce(to_char(to_date(cp.fecha_tnm_retiro,'DD/MM/YYYY'), 'DD/MM/YYYY'),'') else
    '' end as fecha_tnm_retiro 
	,case when t.fk_proforma is not null then
	coalesce(to_char(cp.fecha_retiro_puerto, 'DD/MM/YYYY'),'') else
    '' end as fecha_retiro_puerto 
	,coalesce((SELECT coalesce(CONCAT(u2.nombre,' ',u2.apellidos),'') FROM usuario u2 inner join public.tracking_entrega te on u2.id=te.fk_usuario where te.fk_tracking=t.id and te.estado=true order by te.id desc limit 1),'') as responsable_entrega
	/*,CASE
      WHEN te.id IS NOT NULL THEN
      coalesce(CONCAT(u2.nombre,' ',u2.apellidos),'')
      ELSE
      null 
      END as responsable_entrega*/
	  ,coalesce((select nc."m3" from public.notas_cobros nc inner join public.despachos d on d.id=nc.fk_despacho where t.fk_cliente=d.fk_cliente and t.fk_proforma=d.fk_proforma and d.estado=true order by d.id desc limit 1), 0) as m3
	  /*
	 ,case when nc."m3" is not null then
	nc."m3" else
    0 end as m3
	,case when s.id_consolidado_comercial is not null and s.id_consolidado_comercial!='' then
	coalesce(to_char(d."createdAt", 'DD/MM/YYYY'),'') else
    '' end as fecha_envio_nc*/
	,case when s.id_consolidado_comercial is not null and s.id_consolidado_comercial!='' then
	coalesce(to_char((select d."createdAt" from public.despachos d where t.fk_cliente=d.fk_cliente and t.fk_proforma=d.fk_proforma and d.estado=true order by d.id desc limit 1), 'DD/MM/YYYY'),'') else
    '' end as fecha_envio_nc
	,case when t.fecha_recepcion_1 is not null then
	 coalesce(to_char(to_date(t.fecha_recepcion_1, 'DD/MM/YYYY'), 'DD/MM/YYYY'),'') else
    '' end as fecha_recepcion_1
	,case when t.fecha_recepcion_2 is not null then
	 coalesce(to_char(to_date(t.fecha_recepcion_2, 'DD/MM/YYYY'), 'DD/MM/YYYY'),'') else
    '' end as fecha_recepcion_2
	,case when t.fecha_recepcion_3 is not null then
	 coalesce(to_char(to_date(t.fecha_recepcion_3, 'DD/MM/YYYY'), 'DD/MM/YYYY'),'') else
    '' end as fecha_recepcion_3
	,case when t.fecha_recepcion_4 is not null then
	 coalesce(to_char(to_date(t.fecha_recepcion_4, 'DD/MM/YYYY'), 'DD/MM/YYYY'),'') else
    '' end as fecha_recepcion_4
	,case when t.fecha_recepcion_5 is not null then
	 coalesce(to_char(to_date(t.fecha_recepcion_5, 'DD/MM/YYYY'), 'DD/MM/YYYY'),'') else
    '' end as fecha_recepcion_5
	/*,case when nc.fecha_pago_1 is not null then
	 coalesce(to_char(to_date(nc.fecha_pago_1, 'DD/MM/YYYY'), 'DD/MM/YYYY'),'') else
    '' end as fecha_pago_1
	,case when nc.fecha_pago_2 is not null then
	 coalesce(to_char(to_date(nc.fecha_pago_2, 'DD/MM/YYYY'), 'DD/MM/YYYY'),'') else
    '' end as fecha_pago_2
	,case when nc.fecha_pago_3 is not null then
	 coalesce(to_char(to_date(nc.fecha_pago_3, 'DD/MM/YYYY'), 'DD/MM/YYYY'),'') else
    '' end as fecha_pago_3
	,case when nc.fecha_pago_4 is not null then
	 coalesce(to_char(to_date(nc.fecha_pago_4, 'DD/MM/YYYY'), 'DD/MM/YYYY'),'') else
    '' end as fecha_pago_4
	,case when nc.fecha_pago_5 is not null then
	 coalesce(to_char(to_date(nc.fecha_pago_5, 'DD/MM/YYYY'), 'DD/MM/YYYY'),'') else
    '' end as fecha_pago_5*/
    ,
	DATE_PART('day',(cp.fecha_retiro_puerto + cnt.dias_libres * interval '1 day' - CURRENT_TIMESTAMP))  as dias_libres_restantes
	, coalesce(t.fecha_pago_1,'') as fecha_pago_1
	, coalesce(t.fecha_pago_2,'') as fecha_pago_2
	, coalesce(t.fecha_pago_3,'') as fecha_pago_3
	, coalesce(t.fecha_pago_4,'') as fecha_pago_4
	, coalesce(t.fecha_pago_5,'') as fecha_pago_5
	
	
    FROM public.sla_00_completo s
	INNER JOIN public.clientes c on c.id=s.id_cliente::integer
	LEFT JOIN public.usuario u on u.id=c.fk_ejecutivocuenta
	LEFT JOIN public.tracking_despacho_retiro tdr on tdr.fk_tracking=s.tracking_id::integer and tdr.estado=true
	LEFT JOIN public.clientes_direcciones as dir on dir.id=tdr.fk_registro_direccion
	LEFT JOIN public.comunas on comunas.id=dir.fk_comuna
	LEFT JOIN public.region on region.id=dir.fk_region
	LEFT JOIN public.tracking t on t.id=s.tracking_id::integer
	LEFT JOIN public.contenedor_proforma cp on cp.id=t.fk_proforma
    LEFT JOIN public.contenedor cnt on cnt.id=cp.fk_contenedor
	/*LEFT JOIN public.tracking_entrega te on te.fk_tracking=t.id::integer and te.estado=true*/

	/*LEFT JOIN public.usuario u2 on u2.id=te.fk_usuario */
	LEFT JOIN public.consolidado cns on cns.id=s.id_consolidado_comercial::integer
	/*LEFT JOIN public.despachos d on d.fk_cliente=t.fk_cliente and d.fk_proforma=t.fk_proforma and d.estado=true
	LEFT JOIN public.notas_cobros nc on nc.fk_despacho=d.id and nc.estado=true order by t.id asc*/
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
                hoja_1.cell(1,1).string('id_proveedor').style(estilo_cabecera).style(celda_izquierda);
                hoja_1.cell(1,2).string('nombre_proveedor').style(estilo_cabecera).style(celda_medio);
                hoja_1.cell(1,3).string('fecha_creacion_proveedor').style(estilo_cabecera).style(celda_medio);
                hoja_1.cell(1,4).string('id_cliente').style(estilo_cabecera).style(celda_medio);
                hoja_1.cell(1,5).string('razon_social_cliente').style(estilo_cabecera).style(celda_medio);
                hoja_1.cell(1,6).string('ejecutivo_comercial').style(estilo_cabecera).style(celda_medio);
                hoja_1.cell(1,7).string('ejecutivo_cuenta').style(estilo_cabecera).style(celda_medio);
                hoja_1.cell(1,8).string('bultos_esperados').style(estilo_cabecera).style(celda_medio);
                hoja_1.cell(1,9).string('m3_esperados').style(estilo_cabecera).style(celda_medio);
                hoja_1.cell(1,10).string('peso_esperado').style(estilo_cabecera).style(celda_medio);
                hoja_1.cell(1,11).string('bultos_recepcionados').style(estilo_cabecera).style(celda_medio);
                hoja_1.cell(1,12).string('bodega_recepcion').style(estilo_cabecera).style(celda_medio);
                hoja_1.cell(1,13).string('fecha_ultima_carga_documentos').style(estilo_cabecera).style(celda_medio);
                hoja_1.cell(1,14).string('fecha_ultima_recepcion_registro_1').style(estilo_cabecera).style(celda_medio);
                hoja_1.cell(1,15).string('fecha_ultima_recepcion_registro_2').style(estilo_cabecera).style(celda_medio);
                hoja_1.cell(1,16).string('fecha_ultima_recepcion_registro_3').style(estilo_cabecera).style(celda_medio);
                hoja_1.cell(1,17).string('fecha_ultima_recepcion_registro_4').style(estilo_cabecera).style(celda_medio);
                hoja_1.cell(1,18).string('fecha_ultima_recepcion_registro_5').style(estilo_cabecera).style(celda_medio);
                hoja_1.cell(1,19).string('fecha_de_creacion_del_consolidado').style(estilo_cabecera).style(celda_medio);
                hoja_1.cell(1,20).string('fecha_cierre_consolidado_comercial').style(estilo_cabecera).style(celda_medio);
                hoja_1.cell(1,21).string('id_consolidado_comercial').style(estilo_cabecera).style(celda_medio);
                hoja_1.cell(1,22).string('tracking_id').style(estilo_cabecera).style(celda_medio);
                hoja_1.cell(1,23).string('proforma_id').style(estilo_cabecera).style(celda_medio);
                hoja_1.cell(1,24).string('fecha_consolidado_contenedor').style(estilo_cabecera).style(celda_medio);
                hoja_1.cell(1,25).string('fecha_ingreso_datos_contenedor_nave_eta').style(estilo_cabecera).style(celda_medio);
                hoja_1.cell(1,26).string('n_contenedor').style(estilo_cabecera).style(celda_medio);
                hoja_1.cell(1,27).string('despacho_id').style(estilo_cabecera).style(celda_medio);
                hoja_1.cell(1,28).string('nombre_nave').style(estilo_cabecera).style(celda_medio);
                hoja_1.cell(1,29).string('etd_nave_asignada').style(estilo_cabecera).style(celda_medio);
                hoja_1.cell(1,30).string('fecha_nueva_etd_o_eta').style(estilo_cabecera).style(celda_medio);
                hoja_1.cell(1,31).string('eta').style(estilo_cabecera).style(celda_medio);
                hoja_1.cell(1,32).string('n_carpeta').style(estilo_cabecera).style(celda_medio);
                hoja_1.cell(1,33).string('fecha_real_etd').style(estilo_cabecera).style(celda_derecha);
                hoja_1.cell(1,34).string('dias_libres_restantes').style(estilo_cabecera).style(celda_derecha);
                hoja_1.cell(1,35).string('fecha_prog_aforo').style(estilo_cabecera).style(celda_medio);
                hoja_1.cell(1,36).string('fecha_real_aforo').style(estilo_cabecera).style(celda_medio);
                hoja_1.cell(1,37).string('fecha_real_eta').style(estilo_cabecera).style(celda_medio);
                hoja_1.cell(1,38).string('fecha_retiro_puerto').style(estilo_cabecera).style(celda_medio);
                hoja_1.cell(1,39).string('hora_retiro').style(estilo_cabecera).style(celda_medio);
                hoja_1.cell(1,40).string('fecha_desconsolidacion_pudahuel').style(estilo_cabecera).style(celda_medio);
                hoja_1.cell(1,41).string('hora_desconsolidacion').style(estilo_cabecera).style(celda_medio);
                hoja_1.cell(1,42).string('estado_finanzas').style(estilo_cabecera).style(celda_medio);
                hoja_1.cell(1,43).string('fecha_de_pago_registro_1').style(estilo_cabecera).style(celda_medio);
                hoja_1.cell(1,44).string('fecha_de_pago_registro_2').style(estilo_cabecera).style(celda_medio);
                hoja_1.cell(1,45).string('fecha_de_pago_registro_3').style(estilo_cabecera).style(celda_medio);
                hoja_1.cell(1,46).string('fecha_de_pago_registro_4').style(estilo_cabecera).style(celda_medio);
                hoja_1.cell(1,47).string('fecha_de_pago_registro_5').style(estilo_cabecera).style(celda_medio);
                hoja_1.cell(1,48).string('fecha_solicitud_despacho').style(estilo_cabecera).style(celda_medio);
                hoja_1.cell(1,49).string('fecha_prog_despacho').style(estilo_cabecera).style(celda_medio);
                hoja_1.cell(1,50).string('direccion_entrega').style(estilo_cabecera).style(celda_medio);
                hoja_1.cell(1,51).string('comuna').style(estilo_cabecera).style(celda_medio);
                hoja_1.cell(1,52).string('tipo_entrega').style(estilo_cabecera).style(celda_medio);
                hoja_1.cell(1,53).string('fecha_listo_para_entrega').style(estilo_cabecera).style(celda_medio);
                hoja_1.cell(1,54).string('fecha_entrega').style(estilo_cabecera).style(celda_medio);
                hoja_1.cell(1,55).string('estado_entrega').style(estilo_cabecera).style(celda_medio);
                hoja_1.cell(1,56).string('chofer').style(estilo_cabecera).style(celda_medio);
                hoja_1.cell(1,57).string('dias_libres').style(estilo_cabecera).style(celda_medio);
                hoja_1.cell(1,58).string('fecha_creacion_cliente').style(estilo_cabecera).style(celda_medio);
                hoja_1.cell(1,59).string('m3_consolidados_nc').style(estilo_cabecera).style(celda_derecha);
                hoja_1.cell(1,60).string('fecha_envio_nc').style(estilo_cabecera).style(celda_derecha);
                hoja_1.cell(1,61).string('responsable_entrega').style(estilo_cabecera).style(celda_derecha);
                

                
                for(var i=0; i<Reporte.rows.length; i++)
                {
                    row++;
                    col = 1;

                    hoja_1.cell(row,col).string(''+Reporte.rows[i]['id_proveedor'].toString()).style(estilo_contenido_texto).style(celda_izquierda); col++;
                    hoja_1.cell(row,col).string(''+Reporte.rows[i]['nombre_proveedor'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                    hoja_1.cell(row,col).string(''+Reporte.rows[i]['fecha_creacion_proveedor'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                    hoja_1.cell(row,col).string(''+Reporte.rows[i]['id_cliente'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                    hoja_1.cell(row,col).string(''+Reporte.rows[i]['razon_social_cliente'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                    hoja_1.cell(row,col).string(''+Reporte.rows[i]['ejecutivo_comercial'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                    hoja_1.cell(row,col).string(''+Reporte.rows[i]['ejecutivo_cuenta'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                    hoja_1.cell(row,col).string(''+Reporte.rows[i]['bultos_esperados'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                    hoja_1.cell(row,col).string(''+Reporte.rows[i]['m3_esperados'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                    hoja_1.cell(row,col).string(''+Reporte.rows[i]['peso_esperado'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                    hoja_1.cell(row,col).string(''+Reporte.rows[i]['bultos_recepcionados'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                    hoja_1.cell(row,col).string(''+Reporte.rows[i]['bodega_recepcion'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                    hoja_1.cell(row,col).string(''+Reporte.rows[i]['fecha_ultima_carga_documentos'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                    hoja_1.cell(row,col).string(Reporte.rows[i]['fecha_recepcion_1']==null ? '':''+Reporte.rows[i]['fecha_recepcion_1'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                    hoja_1.cell(row,col).string(Reporte.rows[i]['fecha_recepcion_2']==null ? '':''+Reporte.rows[i]['fecha_recepcion_2'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                    hoja_1.cell(row,col).string(Reporte.rows[i]['fecha_recepcion_3']==null ? '':''+Reporte.rows[i]['fecha_recepcion_3'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                    hoja_1.cell(row,col).string(Reporte.rows[i]['fecha_recepcion_4']==null ? '':''+Reporte.rows[i]['fecha_recepcion_4'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                    hoja_1.cell(row,col).string(Reporte.rows[i]['fecha_recepcion_5']==null ? '':''+Reporte.rows[i]['fecha_recepcion_5'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                    console.log(19);
                    hoja_1.cell(row,col).string(''+Reporte.rows[i]['fecha_de_creacion_del_consolidado'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                    console.log(20);
                    hoja_1.cell(row,col).string(''+Reporte.rows[i]['fecha_cierre_consolidado_comercial'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                    console.log(21);
                    hoja_1.cell(row,col).string(''+Reporte.rows[i]['id_consolidado_comercial'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                    console.log(22);
                    hoja_1.cell(row,col).string(''+Reporte.rows[i]['tracking_id'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                    console.log(23);
                    hoja_1.cell(row,col).string(''+Reporte.rows[i]['proforma_id'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                    console.log(24);
                    hoja_1.cell(row,col).string(''+Reporte.rows[i]['fecha_consolidado_contenedor'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                    console.log(25);
                    hoja_1.cell(row,col).string(''+Reporte.rows[i]['fecha_ingreso_datos_contenedor_nave_eta'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                    console.log(26);
                    hoja_1.cell(row,col).string(''+Reporte.rows[i]['n_contenedor'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                    console.log(27);
                    hoja_1.cell(row,col).string(''+Reporte.rows[i]['despacho_id'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                    console.log(28);
                    hoja_1.cell(row,col).string(''+Reporte.rows[i]['nombre_nave'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                    console.log(29);
                    hoja_1.cell(row,col).string(''+Reporte.rows[i]['etd_nave_asignada'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                    console.log(30);
                    hoja_1.cell(row,col).string(''+Reporte.rows[i]['fecha_nueva_etd_o_eta'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                    console.log(31);
                    hoja_1.cell(row,col).string(''+Reporte.rows[i]['eta'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                    console.log(32);
                    hoja_1.cell(row,col).string(''+Reporte.rows[i]['n_carpeta'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                    console.log(33);
                    hoja_1.cell(row,col).string(''+Reporte.rows[i]['fecha_real_etd'].toString()).style(estilo_contenido_texto).style(celda_derecha); col++;
                    console.log(34);
                    hoja_1.cell(row,col).string(Reporte.rows[i]['dias_libres_restantes']==null ? '':''+Reporte.rows[i]['dias_libres_restantes'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                    console.log(35);
                    hoja_1.cell(row,col).string(''+Reporte.rows[i]['fecha_publicacion_aforo'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                    console.log(36);
                    hoja_1.cell(row,col).string(''+Reporte.rows[i]['fecha_real_aforo'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                    console.log(37);
                    hoja_1.cell(row,col).string(''+Reporte.rows[i]['fecha_real_eta'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                    console.log(38);
                    hoja_1.cell(row,col).string(''+Reporte.rows[i]['fecha_retiro_puerto'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                    console.log(39);
                    hoja_1.cell(row,col).string(''+Reporte.rows[i]['hora_retiro'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                    console.log(40);
                    hoja_1.cell(row,col).string(Reporte.rows[i]['fecha_listo_entrega']==null ? '':''+Reporte.rows[i]['fecha_listo_entrega'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                    console.log(41);
                    hoja_1.cell(row,col).string(Reporte.rows[i]['hora_listo_entrega']==null ? '':''+Reporte.rows[i]['hora_listo_entrega'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                    console.log(42);
                    hoja_1.cell(row,col).string(''+Reporte.rows[i]['estado_finanzas'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                    console.log(43);
                    hoja_1.cell(row,col).string(Reporte.rows[i]['fecha_pago_1']==null ? '':''+Reporte.rows[i]['fecha_pago_1'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                    console.log(44);
                    hoja_1.cell(row,col).string(Reporte.rows[i]['fecha_pago_2']==null ? '':''+Reporte.rows[i]['fecha_pago_2'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                    console.log(45);
                    hoja_1.cell(row,col).string(Reporte.rows[i]['fecha_pago_3']==null ? '':''+Reporte.rows[i]['fecha_pago_3'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                    console.log(46);
                    hoja_1.cell(row,col).string(Reporte.rows[i]['fecha_pago_4']==null ? '':''+Reporte.rows[i]['fecha_pago_4'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                    console.log(47);
                    hoja_1.cell(row,col).string(Reporte.rows[i]['fecha_pago_5']==null ? '':''+Reporte.rows[i]['fecha_pago_5'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                    console.log(48);
                    hoja_1.cell(row,col).string(Reporte.rows[i]['fecha_solicitud_despacho']==null ? '':''+Reporte.rows[i]['fecha_solicitud_despacho'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                    console.log(49);
                    hoja_1.cell(row,col).string(Reporte.rows[i]['fecha_programada']==null ? '':''+Reporte.rows[i]['fecha_programada'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                    console.log(50);
                    hoja_1.cell(row,col).string(Reporte.rows[i]['fk_direccion_completa']==null ? '':''+Reporte.rows[i]['fk_direccion_completa'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                    console.log(51);
                    hoja_1.cell(row,col).string(Reporte.rows[i]['fk_comuna_nombre']==null ? '':''+Reporte.rows[i]['fk_comuna_nombre'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                    console.log(52);
                    hoja_1.cell(row,col).string(Reporte.rows[i]['tipo_entrega']==null ? '':''+Reporte.rows[i]['tipo_entrega'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                    console.log(53);
                    hoja_1.cell(row,col).string(Reporte.rows[i]['fecha_listo_entrega']==null ? '':''+Reporte.rows[i]['fecha_listo_entrega'].toString()).style(estilo_contenido_texto).style(celda_derecha); col++;
                    console.log(54);
                    console.log(56);
                    hoja_1.cell(row,col).string(''+Reporte.rows[i]['fecha_entrega'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                    console.log(57);
                    hoja_1.cell(row,col).string(''+Reporte.rows[i]['estado_entrega'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                    console.log(58);
                    hoja_1.cell(row,col).string(''+Reporte.rows[i]['chofer'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                    console.log(59);
                    hoja_1.cell(row,col).string(Reporte.rows[i]['dias_libres']==null ? '':''+Reporte.rows[i]['dias_libres'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                    console.log(60);
                    hoja_1.cell(row,col).string(''+Reporte.rows[i]['fecha_creacion_cliente'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                    console.log(61);
                    hoja_1.cell(row,col).string(''+Reporte.rows[i]['m3_recibidos'].toString()).style(estilo_contenido_texto).style(celda_derecha); col++;
                    console.log(62);
                    hoja_1.cell(row,col).string(''+Reporte.rows[i]['fecha_envio_nc'].toString()).style(estilo_contenido_texto).style(celda_derecha); col++;
                    console.log(63);
                    hoja_1.cell(row,col).string(Reporte.rows[i]['responsable_entrega']==null ? '':''+Reporte.rows[i]['responsable_entrega'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                    console.log(64);
                }

            } catch (error) {
                await client.query(` update public.excel_despachos set mensaje_carga = concat(mensaje_carga,'\n\n03.- ERROR `+error+`') `);
                console.log("\nERROR "+error);
                res.status(400).send({
                    message: "ERROR AL CREAR EXCEL",
                    success:false,
                }); res.end(); res.connection.destroy();
            }

            console.log('\n.::.');
            console.log('\n.::.');
            console.log('\n.::.');
            console.log('\nGuardando excel ');


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
                    console.log("\nERROR "+err);
                } else {
                }
            });
        }

    }
};

async function updateDataSla00(){
    try{
    let sql1=`UPDATE public.sla_00_completo 
    set ejecutivo_cuenta=(
    SELECT CONCAT(coalesce(u.nombre,''),' ',coalesce(u.apellidos,''))  from public.usuario u inner join public.clientes c on c.fk_ejecutivocuenta=u.id where c.id=public.sla_00_completo.id_cliente::integer and c.fk_ejecutivocuenta is not null
    ) where ejecutivo_cuenta is null;`;

    let sql2=`UPDATE public.sla_00_completo
    set fecha_solicitud_despacho=(
    SELECT to_char( tdr.fecha, 'DD/MM/YYYY') from public.tracking_despacho_retiro tdr where tdr.fk_tracking=public.sla_00_completo.tracking_id::integer and tdr.estado=true order by id desc limit 1
    ) where fecha_solicitud_despacho is null;`;

    let sql3=`UPDATE public.sla_00_completo
    set fecha_prog_despacho=(
    SELECT to_char( te.fecha_programada, 'DD/MM/YYYY') from public.tracking_entrega te where te.fk_tracking=public.sla_00_completo.tracking_id::integer and te.estado=true order by id desc limit 1
    ) where fecha_prog_despacho is null;`;

    let sql4=`UPDATE public.sla_00_completo
    set estado_entrega=(
        SELECT CASE WHEN te.estado_entrega=1 then 'ENTREGADO' WHEN te.estado_entrega=2 then 'PARCIAL' ELSE null END
        FROM public.tracking_entrega te where te.fk_tracking=public.sla_00_completo.tracking_id::integer and te.estado=true order by id desc limit 1	
    ) where estado_entrega is null;`;

    let sql5=`UPDATE public.sla_00_completo
    set chofer=(
        SELECT CONCAT(coalesce(te.conductor_nombre,''),' ',coalesce(te.conductor_apellido,'')) FROM public.tracking_entrega te where te.fk_tracking=public.sla_00_completo.tracking_id::integer and te.estado=true and te.conductor_nombre is not null order by id desc limit 1
    ) where chofer is null;`;

    let sql6=`UPDATE public.sla_00_completo
    set fecha_aprobacion_documentos=(
        SELECT to_char( t.fecha_ultima_carga_documento, 'DD/MM/YYYY') FROM public.tracking t where t.id=public.sla_00_completo.tracking_id::integer
    ) where fecha_aprobacion_documentos is null;`;

    let sql7=`UPDATE public.sla_00_completo
    set tipo_de_entrega=(
        SELECT CASE 
        WHEN tdr.fk_bodega is not null then 'RETIRO' 
        WHEN tdr.empresa_ext_retiro is not null then 'RETIRA TRANS.EXTERNO' 
        WHEN tdr.empresa_ext_despacho is not null then 'DESPACHO TRANS.EXTERNO' 
        WHEN tdr.fk_registro_direccion is null and tdr.fk_bodega is null and tdr.empresa_ext_retiro is null and tdr.empresa_ext_despacho is null then 'SIN ESPECIFICAR'
        WHEN tdr.fk_registro_direccion is not null and cd.fk_region!=12 and cd.fk_region is not null then 'REVISAR DESPACHO GRATUITO NO INCLUIDO'
        WHEN tdr.fk_registro_direccion is not null and cd.fk_region=12 and cd.fk_region is not null and cd.fk_comuna in(49,50,51,53,57,59,61,62,64,65,66,69,71,72,76,82,88,91) then 'REVISAR DESPACHO GRATUITO NO INCLUIDO'
        WHEN tdr.fk_registro_direccion is not null and cd.fk_region=12 and cd.fk_region is not null and cd.fk_comuna not in(49,50,51,53,57,59,61,62,64,65,66,69,71,72,76,82,88,91) then 'DESPACHO GRATIS INCLUIDO'
        ELSE 'SIN ESPECIFICAR' END
        FROM public.tracking_despacho_retiro tdr 
        left join public.clientes_direcciones cd on cd.id=tdr.fk_registro_direccion
        where tdr.fk_tracking=public.sla_00_completo.tracking_id::integer and tdr.estado=true order by tdr.id desc limit 1	
    ) where tipo_de_entrega is null;`;

    let sql8=`UPDATE public.sla_00_completo
    set direccion_entrega=(
        SELECT CASE 
        WHEN tdr.fk_bodega is not null then 'Camino a noviciado 1945, Bodega 19.'
        WHEN tdr.empresa_ext_retiro is not null then CONCAT(coalesce(tdr.calle_empresa_ext,''),' ',coalesce(tdr.numeracion_empresa_ext,''))
        WHEN tdr.empresa_ext_despacho is not null then CONCAT(coalesce(tdr.calle_empresa_ext,''),' ',coalesce(tdr.numeracion_empresa_ext,''))
        WHEN tdr.fk_registro_direccion is not null then CONCAT(coalesce(cd.direccion,''),' ',coalesce(cd.numero,''))
        WHEN tdr.fk_registro_direccion is null and tdr.fk_bodega is null and tdr.empresa_ext_retiro is null and tdr.empresa_ext_despacho is null then 'SIN ESPECIFICAR'
        ELSE 'SIN ESPECIFICAR' END
        FROM public.tracking_despacho_retiro tdr 
        left join public.clientes_direcciones cd on cd.id=tdr.fk_registro_direccion
        where tdr.fk_tracking=public.sla_00_completo.tracking_id::integer and tdr.estado=true order by tdr.id desc limit 1
    ) where direccion_entrega is null;`;

    let sql9=`UPDATE public.sla_00_completo
    set comuna=(
        SELECT CASE 
        WHEN tdr.fk_bodega is not null then 'PUDAHUEL'
        WHEN tdr.empresa_ext_retiro is not null then CONCAT(coalesce(c2.nombre,''))
        WHEN tdr.empresa_ext_despacho is not null then CONCAT(coalesce(c2.nombre,''))
        WHEN tdr.fk_registro_direccion is not null then CONCAT(coalesce(c1.nombre,''))
        WHEN tdr.fk_registro_direccion is null and tdr.fk_bodega is null and tdr.empresa_ext_retiro is null and tdr.empresa_ext_despacho is null then 'SIN ESPECIFICAR'
        ELSE 'SIN ESPECIFICAR' END
        FROM public.tracking_despacho_retiro tdr 
        left join public.clientes_direcciones cd on cd.id=tdr.fk_registro_direccion
        left join public.comunas c1 on c1.id=cd.fk_comuna
        left join public.comunas c2 on c2.id=tdr.fk_comuna_empresa_ext
        where tdr.fk_tracking=public.sla_00_completo.tracking_id::integer and tdr.estado=true order by tdr.id desc limit 1
    ) where comuna is null;`;

    let sql10=`UPDATE public.sla_00_completo
    set fecha_publicacion_aforo=(
        SELECT to_char( cp.fecha_prog_aforo, 'DD/MM/YYYY') FROM public.contenedor_proforma cp 
        INNER JOIN public.tracking t on t.fk_proforma=cp.id
        where t.id=public.sla_00_completo.tracking_id::integer
    ) where fecha_publicacion_aforo is null;`;

    let sql11=`UPDATE public.sla_00_completo
    set fecha_retiro_puerto=(
        SELECT to_char( cp.fecha_retiro_puerto, 'DD/MM/YYYY') FROM public.contenedor_proforma cp 
        INNER JOIN public.tracking t on t.fk_proforma=cp.id
        where t.id=public.sla_00_completo.tracking_id::integer
    ) where fecha_retiro_puerto is null`;

    console.log('\n antes de sql1 ');
    await client.query(sql1);
    console.log('\n despues de sql1 ');
    console.log('\n antes de sql2 ');
    await client.query(sql2);
    console.log('\n despues de sql2 ');
    console.log('\n antes de sql3 ');
    await client.query(sql3);
    console.log('\n despues de sql3 ');
    console.log('\n antes de sql4 ');
    await client.query(sql4);
    console.log('\n despues de sql4 ');
    console.log('\n antes de sql5 ');
    await client.query(sql5);
    console.log('\n despues de sql5 ');
    console.log('\n antes de sql6 ');
    await client.query(sql6);
    console.log('\n despues de sql6 ');
    console.log('\n antes de sql7 ');
    await client.query(sql7);
    console.log('\n despues de sql7 ');
    console.log('\n antes de sql8 ');
    await client.query(sql8);
    console.log('\n despues de sql8 ');
    console.log('\n antes de sql9 ');
    await client.query(sql9);
    console.log('\n despues de sql9 ');
    console.log('\n antes de sql10 ');
    await client.query(sql10);
    console.log('\n despues de sql10 ');
    console.log('\n antes de sql11 ');
    await client.query(sql11);
    console.log('\n despues de sql11 ');
    }catch(error){
        console.log('\nSCRIPT ACTUALIZACION SLA 00 COMPLETO: '+error);
    }
}

exports.GenerateReporteGatiloByAPI = async (req,res) =>{ try{
    let result=await client.query(`SELECT id FROM public.tracking where fecha_recepcion_1 is null and estado>=0 order by id asc`);
    if(result && result.rows && result.rows.length>0){
        for(i=0;i<result.rows.length;i++){
            let r=await client.query(`select
            distinct fecha_recepcion,
            to_char( fecha_recepcion, 'DD/MM/YYYY') as fecha_format
            from public.tracking_detalle
            where tracking_id=`+result.rows[i].id+` order by fecha_recepcion asc`);
            console.log(result.rows[i].id);
            if(r && r.rows && r.rows.length>0){
                //if(r.rows.length<=5){
                    for(x=0;x<r.rows.length;x++){
                        if(x==0){
                            await client.query(`UPDATE public.tracking SET fecha_recepcion_1='`+r.rows[x].fecha_format+`' where id=`+result.rows[i].id);
                        }else if(x==1){
                            await client.query(`UPDATE public.tracking SET fecha_recepcion_2='`+r.rows[x].fecha_format+`' where id=`+result.rows[i].id);
                        }else if(x==2){
                            await client.query(`UPDATE public.tracking SET fecha_recepcion_3='`+r.rows[x].fecha_format+`' where id=`+result.rows[i].id);
                        }else if(x==3){
                            await client.query(`UPDATE public.tracking SET fecha_recepcion_4='`+r.rows[x].fecha_format+`' where id=`+result.rows[i].id);
                        }else if(x==4){
                            await client.query(`UPDATE public.tracking SET fecha_recepcion_5='`+r.rows[x].fecha_format+`' where id=`+result.rows[i].id);
                        }
                    }
                /*}else if(r.rows.length>5){
                    console.log('\nmas de 5:'+result.rows[i].id);
                    for(x=0;x<4;x++){
                        if(x==0){
                            await client.query(`UPDATE public.tracking SET fecha_recepcion_1='`+r.rows[x].fecha_format+`' where id=`+result.rows[i].id);
                        }else if(x==1){
                            await client.query(`UPDATE public.tracking SET fecha_recepcion_2='`+r.rows[x].fecha_format+`' where id=`+result.rows[i].id);
                        }else if(x==2){
                            await client.query(`UPDATE public.tracking SET fecha_recepcion_3='`+r.rows[x].fecha_format+`' where id=`+result.rows[i].id);
                        }else if(x==3){
                            await client.query(`UPDATE public.tracking SET fecha_recepcion_4='`+r.rows[x].fecha_format+`' where id=`+result.rows[i].id);
                        }	
                    }
                    await client.query(`UPDATE public.tracking SET fecha_recepcion_5='`+r.rows[r.rows.length-1].fecha_format+`' where id=`+result.rows[i].id);
                }*/
            }
            if(i==result.rows.length-1){
                console.log('\nultimo');
            }
        }
        console.log('\nterminado paso 1');
    }
    var fecha_carga = moment().format("DD-MM-YYYY HH:mm");
    await client.query(`update public.excel_despachos set mensaje_carga ='01.- ARCHIVO CARGADO',fecha_carga='`+fecha_carga+`', link_archivo=''`);

    
    var ExistenDatos = await client.query(` select * FROM public.excel_despachos WHERE mensaje_carga='01.- ARCHIVO CARGADO'`);

    if(ExistenDatos && ExistenDatos.rows && ExistenDatos.rows.length>0)
    {

        await client.query(` update public.excel_despachos set mensaje_carga = concat(mensaje_carga,'\n\n02.- PROCESANDO DATOS PARA REPORTE') `);

        await client.query(` DELETE FROM public.sla_00_completo where estado_despacho!='ENTREGADO' `);

        console.log('\n.::.');
        console.log('\n.::.');
        console.log('\n.::.');
        console.log('\nInsertando el reporte ');
        await client.query(`
        
        DO $$
DECLARE
    id_val INT;
BEGIN
    -- Recorremos los valores de tck.id del 1 al 100
    FOR id_val IN 1..100000 LOOP
	
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
		and tck.id=id_val
		
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
        , exc.dias_libres;
		
	END LOOP;		
END $$;
        `);

        await client.query(`
        UPDATE tracking a SET estado_sla_00 = true WHERE estado_sla_00 is not true and EXISTS (SELECT FROM sla_00_completo b WHERE b.tracking_id::text = a.id::text and b.estado_despacho='ENTREGADO')
        `);

        await client.query(` update public.excel_despachos set mensaje_carga = concat(mensaje_carga,'\n\n03.- CAPTURADO DATOS PARA ARCHIVO DE REPORTE') `);

        console.log('\n.::.');
        console.log('\n.::.');
        console.log('\n.::.');
        console.log('\nCapturando el porte');
        var Reporte = await client.query(`
        SELECT DISTINCT t.id as tracking_id,
coalesce(s.id_proveedor,'') as id_proveedor
, coalesce(s.nombre_proveedor,'') as nombre_proveedor

, case when length(s.fecha_creacion_proveedor)>0 and s.fecha_creacion_proveedor!='' and s.fecha_creacion_proveedor is not null and s.fecha_creacion_proveedor!='#¡REF!' then
coalesce(to_char(to_date(s.fecha_creacion_proveedor, 'DD/MM/YYYY'), 'DD/MM/YYYY'),'') else
'' end as fecha_creacion_proveedor

, coalesce(s.id_cliente,'') as id_cliente
, coalesce(s.razon_social_cliente,'') as razon_social_cliente
, coalesce(s.ejecutivo,'') as ejecutivo_comercial
,CASE
  WHEN c.fk_ejecutivocuenta IS NOT NULL THEN
  coalesce(CONCAT(u.nombre,' ',u.apellidos),'')
  ELSE
  '' 
  END as ejecutivo_cuenta
, coalesce(s.bultos_esperados::text,'') as bultos_esperados
, REPLACE(coalesce(s.m3_esperados::text,''), '.', ',') as m3_esperados
, REPLACE(coalesce(s.m3_recibidos::text,''), '.', ',') as m3_recibidos
, coalesce(s.peso_esperado::text,'') as peso_esperado
, coalesce(s.bultos_recepcionados::text,'') as bultos_recepcionados
, coalesce(s.bodega_recepcion,'') as bodega_recepcion

, case when s.fecha_ultima_carga_documentos='OK' then s.fecha_ultima_carga_documentos
when length(s.fecha_ultima_carga_documentos)>0 and s.fecha_ultima_carga_documentos!='' and s.fecha_ultima_carga_documentos is not null and s.fecha_ultima_carga_documentos!='#¡REF!' then
coalesce(to_char(to_date(s.fecha_ultima_carga_documentos, 'DD/MM/YYYY'), 'DD/MM/YYYY'),'') else

case when s.fecha_cierre_consolidado_comercial='OK' then s.fecha_cierre_consolidado_comercial
when length(s.fecha_cierre_consolidado_comercial)>0 and s.fecha_cierre_consolidado_comercial!='' and s.fecha_cierre_consolidado_comercial is not null and s.fecha_cierre_consolidado_comercial!='#¡REF!' then
coalesce(to_char(to_date(s.fecha_cierre_consolidado_comercial, 'DD/MM/YYYY'), 'DD/MM/YYYY'),'') else
'' end

end as fecha_ultima_carga_documentos

, coalesce(s.fecha_aprobacion_documentos,'') as fecha_aprobacion_documentos

, case when s.fecha_ultima_recepcion='OK' then s.fecha_ultima_recepcion
when length(s.fecha_ultima_recepcion)>0 and s.fecha_ultima_recepcion!='' and s.fecha_ultima_recepcion is not null and s.fecha_ultima_recepcion!='#¡REF!' then
coalesce(to_char(to_date(s.fecha_ultima_recepcion, 'YYYY/MM/DD'), 'DD/MM/YYYY'),'') else
'' end as fecha_ultima_recepcion_registro_1

, case when s.fecha_de_creacion_del_consolidado='OK' then s.fecha_de_creacion_del_consolidado
when length(s.fecha_de_creacion_del_consolidado)>0 and s.fecha_de_creacion_del_consolidado!='' and s.fecha_de_creacion_del_consolidado is not null and s.fecha_de_creacion_del_consolidado!='#¡REF!' then
coalesce(to_char(to_date(s.fecha_de_creacion_del_consolidado, 'DD/MM/YYYY'), 'DD/MM/YYYY'),'') else
'' end as fecha_de_creacion_del_consolidado

, case when s.fecha_cierre_consolidado_comercial='OK' then s.fecha_cierre_consolidado_comercial
when length(s.fecha_cierre_consolidado_comercial)>0 and s.fecha_cierre_consolidado_comercial!='' and s.fecha_cierre_consolidado_comercial is not null and s.fecha_cierre_consolidado_comercial!='#¡REF!' then
coalesce(to_char(to_date(s.fecha_cierre_consolidado_comercial, 'DD/MM/YYYY'), 'DD/MM/YYYY'),'') else
'' end as fecha_cierre_consolidado_comercial

, coalesce(s.id_consolidado_comercial,'') as id_consolidado_comercial
, coalesce(s.proforma_id,'') as proforma_id

, case when s.fecha_consolidado_contenedor='OK' then s.fecha_consolidado_contenedor
when length(s.fecha_consolidado_contenedor)>0 and s.fecha_consolidado_contenedor!='' and s.fecha_consolidado_contenedor is not null and s.fecha_consolidado_contenedor!='#¡REF!' then
coalesce(to_char(to_date(s.fecha_consolidado_contenedor, 'DD/MM/YYYY'), 'DD/MM/YYYY'),'') else
'' end as fecha_consolidado_contenedor

, case when s.fecha_ingreso_datos_contenedor_nave_eta='OK' then s.fecha_ingreso_datos_contenedor_nave_eta
when length(s.fecha_ingreso_datos_contenedor_nave_eta)>0 and s.fecha_ingreso_datos_contenedor_nave_eta!='' and s.fecha_ingreso_datos_contenedor_nave_eta is not null and s.fecha_ingreso_datos_contenedor_nave_eta!='#¡REF!' then
coalesce(to_char(to_date(s.fecha_ingreso_datos_contenedor_nave_eta, 'YYYY/MM/DD'), 'DD/MM/YYYY'),'') else
'' end as fecha_ingreso_datos_contenedor_nave_eta

, coalesce(s.n_contenedor,'') as n_contenedor
, coalesce(s.despacho_id,'') as despacho_id
, coalesce(s.nombre_nave,'') as nombre_nave

, case when s.etd_nave_asignada='OK' then s.etd_nave_asignada
when length(s.etd_nave_asignada)>0 and s.etd_nave_asignada!='' and s.etd_nave_asignada is not null and s.etd_nave_asignada!='#¡REF!' then
coalesce(to_char(to_date(s.etd_nave_asignada, 'DD/MM/YYYY'), 'DD/MM/YYYY'),'') else
'' end as etd_nave_asignada

, case when s.fecha_nueva_etd_o_eta='OK' then s.fecha_nueva_etd_o_eta
when length(s.fecha_nueva_etd_o_eta)>0 and s.fecha_nueva_etd_o_eta!='' and s.fecha_nueva_etd_o_eta is not null and s.fecha_nueva_etd_o_eta!='#¡REF!' then
coalesce(to_char(to_date(s.fecha_nueva_etd_o_eta, 'DD/MM/YYYY'), 'DD/MM/YYYY'),'') else
'' end as fecha_nueva_etd_o_eta

, case when s.eta='OK' then s.eta
when length(s.eta)>0 and s.eta!='' and s.eta is not null then
coalesce(to_char(to_date(s.eta, 'DD/MM/YYYY'), 'DD/MM/YYYY'),'') else
'' end as eta

, coalesce(s.n_carpeta,'') as n_carpeta

, coalesce(s.fecha_retiro_puerto,'') as fecha_retiro_puerto
, coalesce(s.hora_retiro,'') as hora_retiro

, case when s.fecha_desconsolidacion_pudahuel='OK' then s.fecha_desconsolidacion_pudahuel
when length(s.fecha_desconsolidacion_pudahuel)>0 and s.fecha_desconsolidacion_pudahuel!='' and s.fecha_desconsolidacion_pudahuel is not null and s.fecha_desconsolidacion_pudahuel!='#¡REF!'then 
coalesce(to_char(to_date(s.fecha_desconsolidacion_pudahuel, 'DD/MM/YYYY'), 'DD/MM/YYYY'),'') else
'' end as fecha_desconsolidacion_pudahuel

,case when t.fk_proforma is not null then
coalesce(to_char(cp.fecha_desconsolidado, 'DD/MM/YYYY'),'') else
'' end as fecha_listo_entrega 

 ,case when t.fk_proforma is not null then
coalesce(to_char(cp.fecha_desconsolidado, 'HH24:mm'),'') else
'' end as hora_listo_entrega 

, coalesce(s.hora_desconsolidacion,'') as hora_desconsolidacion
, coalesce(s.estado_finanzas,'') as estado_finanzas

, case when s.fecha_de_pago='OK' then s.fecha_de_pago
when length(s.fecha_de_pago)>0 and s.fecha_de_pago!='' and s.fecha_de_pago is not null and s.fecha_de_pago!='#¡REF!' then
coalesce(to_char(to_date(s.fecha_de_pago, 'DD/MM/YYYY'), 'DD/MM/YYYY'),'') else
'' end as fecha_de_pago_registro_1

, case when s.fecha_ingreso_direccion='OK' then s.fecha_ingreso_direccion
when length(s.fecha_ingreso_direccion)>0 and s.fecha_ingreso_direccion!='' and s.fecha_ingreso_direccion is not null and s.fecha_ingreso_direccion!='#¡REF!' then
coalesce(to_char(to_date(s.fecha_ingreso_direccion, 'DD/MM/YYYY'), 'DD/MM/YYYY'),'') else
'' end as fecha_ingreso_direccion

, case when s.fecha_entrega_retiro='OK' then s.fecha_entrega_retiro
when length(s.fecha_entrega_retiro)>0 and s.fecha_entrega_retiro!='' and s.fecha_entrega_retiro is not null and s.fecha_entrega_retiro!='#¡REF!' then
coalesce(to_char(to_date(s.fecha_entrega_retiro, 'DD/MM/YYYY'), 'DD/MM/YYYY'),'') else
'' end as fecha_entrega
, coalesce(s.chofer,'') as chofer
, cnt.dias_libres

, coalesce(s.fecha_creacion_cliente,'') as fecha_creacion_cliente
, case when tdr.fk_registro_direccion is not null then 
concat(dir.direccion,' ',dir.numero,', ',comunas.nombre,', ',region.nombre) else '' end 
as fk_direccion_completa
,case when tdr.fk_registro_direccion is not null then comunas.nombre else '' end as fk_comuna_nombre
,region.id as fk_region
,case when tdr.fk_registro_direccion is not null then region.nombre else '' end as fk_region_nombre
/*,case when te.fecha_programada is not null then coalesce(to_char(te.fecha_programada, 'DD/MM/YYYY'),'') else
'' end as fecha_programada */
,(SELECT te.fecha_programada FROM tracking_entrega te where te.fk_tracking=t.id and te.estado=true order by te.id desc limit 1) as fecha_programada
,case when tdr.fecha is not null then coalesce(to_char(tdr.fecha, 'DD/MM/YYYY'),'') else
'' end as fecha_solicitud_despacho 
,CASE 
                WHEN tdr.fk_bodega is not null then 'RETIRO' 
                WHEN tdr.empresa_ext_retiro is not null then 'RETIRA TRANS.EXTERNO' 
                WHEN tdr.empresa_ext_despacho is not null then 'DESPACHO TRANS.EXTERNO' 
                WHEN tdr.fk_registro_direccion is null and tdr.fk_bodega is null and tdr.empresa_ext_retiro is null and tdr.empresa_ext_despacho is null then 'SIN ESPECIFICAR'
                WHEN tdr.fk_registro_direccion is not null and dir.fk_region!=12 and dir.fk_region is not null then 'REVISAR DESPACHO GRATUITO NO INCLUIDO'
                WHEN tdr.fk_registro_direccion is not null and dir.fk_region=12 and dir.fk_region is not null and dir.fk_comuna in(49,50,51,53,57,59,61,62,64,65,66,69,71,72,76,82,88,91) then 'REVISAR DESPACHO GRATUITO NO INCLUIDO'
                WHEN tdr.fk_registro_direccion is not null and dir.fk_region=12 and dir.fk_region is not null and dir.fk_comuna not in(49,50,51,53,57,59,61,62,64,65,66,69,71,72,76,82,88,91) then 'DESPACHO GRATIS INCLUIDO'
                ELSE 'SIN ESPECIFICAR' END tipo_entrega
/*,CASE WHEN te.estado_entrega=1 then 'ENTREGADO' WHEN te.estado_entrega=2 then 'PARCIAL' ELSE '' END as estado_entrega*/
,CASE WHEN (SELECT te.estado_entrega FROM tracking_entrega te where te.fk_tracking=t.id and te.estado=true order by te.id desc limit 1)=1 then 'ENTREGADO'
WHEN (SELECT te.estado_entrega FROM tracking_entrega te where te.fk_tracking=t.id and te.estado=true order by te.id desc limit 1)=1 then 'PARCIAL'
ELSE '' END estado_entrega
,case when t.fk_proforma is not null then
coalesce(to_char(cp.fecha_salida_puerto, 'DD/MM/YYYY'),'') else
'' end as fecha_real_etd
,case when t.fk_proforma is not null then
coalesce(to_char(cp.fecha_retiro_puerto, 'DD/MM/YYYY'),'') else
'' end as fecha_real_eta 
,case when t.fk_proforma is not null then
coalesce(to_char(cp.fecha_prog_aforo, 'DD/MM/YYYY'),'') else
'' end as fecha_publicacion_aforo 
,case when t.fk_proforma is not null then
coalesce(to_char(cp.fecha_real_aforo, 'DD/MM/YYYY'),'') else
'' end as fecha_real_aforo 
,case when t.fk_proforma is not null then
coalesce(to_char(to_date(cp.fecha_tnm_retiro,'DD/MM/YYYY'), 'DD/MM/YYYY'),'') else
'' end as fecha_tnm_retiro 
,case when t.fk_proforma is not null then
coalesce(to_char(cp.fecha_retiro_puerto, 'DD/MM/YYYY'),'') else
'' end as fecha_retiro_puerto 
,coalesce((SELECT coalesce(CONCAT(u2.nombre,' ',u2.apellidos),'') FROM usuario u2 inner join public.tracking_entrega te on u2.id=te.fk_usuario where te.fk_tracking=t.id and te.estado=true order by te.id desc limit 1),'') as responsable_entrega
/*,CASE
  WHEN te.id IS NOT NULL THEN
  coalesce(CONCAT(u2.nombre,' ',u2.apellidos),'')
  ELSE
  null 
  END as responsable_entrega*/
  ,coalesce((select nc."m3" from public.notas_cobros nc inner join public.despachos d on d.id=nc.fk_despacho where t.fk_cliente=d.fk_cliente and t.fk_proforma=d.fk_proforma and d.estado=true order by d.id desc limit 1), 0) as m3
  /*
 ,case when nc."m3" is not null then
nc."m3" else
0 end as m3
,case when s.id_consolidado_comercial is not null and s.id_consolidado_comercial!='' then
coalesce(to_char(d."createdAt", 'DD/MM/YYYY'),'') else
'' end as fecha_envio_nc*/
,case when s.id_consolidado_comercial is not null and s.id_consolidado_comercial!='' then
coalesce(to_char((select d."createdAt" from public.despachos d where t.fk_cliente=d.fk_cliente and t.fk_proforma=d.fk_proforma and d.estado=true order by d.id desc limit 1), 'DD/MM/YYYY'),'') else
'' end as fecha_envio_nc
,case when t.fecha_recepcion_1 is not null then
 coalesce(to_char(to_date(t.fecha_recepcion_1, 'DD/MM/YYYY'), 'DD/MM/YYYY'),'') else
'' end as fecha_recepcion_1
,case when t.fecha_recepcion_2 is not null then
 coalesce(to_char(to_date(t.fecha_recepcion_2, 'DD/MM/YYYY'), 'DD/MM/YYYY'),'') else
'' end as fecha_recepcion_2
,case when t.fecha_recepcion_3 is not null then
 coalesce(to_char(to_date(t.fecha_recepcion_3, 'DD/MM/YYYY'), 'DD/MM/YYYY'),'') else
'' end as fecha_recepcion_3
,case when t.fecha_recepcion_4 is not null then
 coalesce(to_char(to_date(t.fecha_recepcion_4, 'DD/MM/YYYY'), 'DD/MM/YYYY'),'') else
'' end as fecha_recepcion_4
,case when t.fecha_recepcion_5 is not null then
 coalesce(to_char(to_date(t.fecha_recepcion_5, 'DD/MM/YYYY'), 'DD/MM/YYYY'),'') else
'' end as fecha_recepcion_5
/*,case when nc.fecha_pago_1 is not null then
 coalesce(to_char(to_date(nc.fecha_pago_1, 'DD/MM/YYYY'), 'DD/MM/YYYY'),'') else
'' end as fecha_pago_1
,case when nc.fecha_pago_2 is not null then
 coalesce(to_char(to_date(nc.fecha_pago_2, 'DD/MM/YYYY'), 'DD/MM/YYYY'),'') else
'' end as fecha_pago_2
,case when nc.fecha_pago_3 is not null then
 coalesce(to_char(to_date(nc.fecha_pago_3, 'DD/MM/YYYY'), 'DD/MM/YYYY'),'') else
'' end as fecha_pago_3
,case when nc.fecha_pago_4 is not null then
 coalesce(to_char(to_date(nc.fecha_pago_4, 'DD/MM/YYYY'), 'DD/MM/YYYY'),'') else
'' end as fecha_pago_4
,case when nc.fecha_pago_5 is not null then
 coalesce(to_char(to_date(nc.fecha_pago_5, 'DD/MM/YYYY'), 'DD/MM/YYYY'),'') else
'' end as fecha_pago_5*/
,
DATE_PART('day',(cp.fecha_retiro_puerto + cnt.dias_libres * interval '1 day' - CURRENT_TIMESTAMP))  as dias_libres_restantes
, coalesce(t.fecha_pago_1,'') as fecha_pago_1
, coalesce(t.fecha_pago_2,'') as fecha_pago_2
, coalesce(t.fecha_pago_3,'') as fecha_pago_3
, coalesce(t.fecha_pago_4,'') as fecha_pago_4
, coalesce(t.fecha_pago_5,'') as fecha_pago_5


FROM public.sla_00_completo s
INNER JOIN public.clientes c on c.id=s.id_cliente::integer
LEFT JOIN public.usuario u on u.id=c.fk_ejecutivocuenta
LEFT JOIN public.tracking_despacho_retiro tdr on tdr.fk_tracking=s.tracking_id::integer and tdr.estado=true
LEFT JOIN public.clientes_direcciones as dir on dir.id=tdr.fk_registro_direccion
LEFT JOIN public.comunas on comunas.id=dir.fk_comuna
LEFT JOIN public.region on region.id=dir.fk_region
LEFT JOIN public.tracking t on t.id=s.tracking_id::integer
LEFT JOIN public.contenedor_proforma cp on cp.id=t.fk_proforma
LEFT JOIN public.contenedor cnt on cnt.id=cp.fk_contenedor
/*LEFT JOIN public.tracking_entrega te on te.fk_tracking=t.id::integer and te.estado=true*/

/*LEFT JOIN public.usuario u2 on u2.id=te.fk_usuario */
LEFT JOIN public.consolidado cns on cns.id=s.id_consolidado_comercial::integer
/*LEFT JOIN public.despachos d on d.fk_cliente=t.fk_cliente and d.fk_proforma=t.fk_proforma and d.estado=true
LEFT JOIN public.notas_cobros nc on nc.fk_despacho=d.id and nc.estado=true order by t.id asc*/
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


            var row = 1;
            var col = 1;
            hoja_1.cell(1,1).string('id_proveedor').style(estilo_cabecera).style(celda_izquierda);
            hoja_1.cell(1,2).string('nombre_proveedor').style(estilo_cabecera).style(celda_medio);
            hoja_1.cell(1,3).string('fecha_creacion_proveedor').style(estilo_cabecera).style(celda_medio);
            hoja_1.cell(1,4).string('id_cliente').style(estilo_cabecera).style(celda_medio);
            hoja_1.cell(1,5).string('razon_social_cliente').style(estilo_cabecera).style(celda_medio);
            hoja_1.cell(1,6).string('ejecutivo_comercial').style(estilo_cabecera).style(celda_medio);
            hoja_1.cell(1,7).string('ejecutivo_cuenta').style(estilo_cabecera).style(celda_medio);
            hoja_1.cell(1,8).string('bultos_esperados').style(estilo_cabecera).style(celda_medio);
            hoja_1.cell(1,9).string('m3_esperados').style(estilo_cabecera).style(celda_medio);
            hoja_1.cell(1,10).string('peso_esperado').style(estilo_cabecera).style(celda_medio);
            hoja_1.cell(1,11).string('bultos_recepcionados').style(estilo_cabecera).style(celda_medio);
            hoja_1.cell(1,12).string('bodega_recepcion').style(estilo_cabecera).style(celda_medio);
            hoja_1.cell(1,13).string('fecha_ultima_carga_documentos').style(estilo_cabecera).style(celda_medio);
            hoja_1.cell(1,14).string('fecha_ultima_recepcion_registro_1').style(estilo_cabecera).style(celda_medio);
            hoja_1.cell(1,15).string('fecha_ultima_recepcion_registro_2').style(estilo_cabecera).style(celda_medio);
            hoja_1.cell(1,16).string('fecha_ultima_recepcion_registro_3').style(estilo_cabecera).style(celda_medio);
            hoja_1.cell(1,17).string('fecha_ultima_recepcion_registro_4').style(estilo_cabecera).style(celda_medio);
            hoja_1.cell(1,18).string('fecha_ultima_recepcion_registro_5').style(estilo_cabecera).style(celda_medio);
            hoja_1.cell(1,19).string('fecha_de_creacion_del_consolidado').style(estilo_cabecera).style(celda_medio);
            hoja_1.cell(1,20).string('fecha_cierre_consolidado_comercial').style(estilo_cabecera).style(celda_medio);
            hoja_1.cell(1,21).string('id_consolidado_comercial').style(estilo_cabecera).style(celda_medio);
            hoja_1.cell(1,22).string('tracking_id').style(estilo_cabecera).style(celda_medio);
            hoja_1.cell(1,23).string('proforma_id').style(estilo_cabecera).style(celda_medio);
            hoja_1.cell(1,24).string('fecha_consolidado_contenedor').style(estilo_cabecera).style(celda_medio);
            hoja_1.cell(1,25).string('fecha_ingreso_datos_contenedor_nave_eta').style(estilo_cabecera).style(celda_medio);
            hoja_1.cell(1,26).string('n_contenedor').style(estilo_cabecera).style(celda_medio);
            hoja_1.cell(1,27).string('despacho_id').style(estilo_cabecera).style(celda_medio);
            hoja_1.cell(1,28).string('nombre_nave').style(estilo_cabecera).style(celda_medio);
            hoja_1.cell(1,29).string('etd_nave_asignada').style(estilo_cabecera).style(celda_medio);
            hoja_1.cell(1,30).string('fecha_nueva_etd_o_eta').style(estilo_cabecera).style(celda_medio);
            hoja_1.cell(1,31).string('eta').style(estilo_cabecera).style(celda_medio);
            hoja_1.cell(1,32).string('n_carpeta').style(estilo_cabecera).style(celda_medio);
            hoja_1.cell(1,33).string('fecha_real_etd').style(estilo_cabecera).style(celda_derecha);
            hoja_1.cell(1,34).string('dias_libres_restantes').style(estilo_cabecera).style(celda_derecha);
            hoja_1.cell(1,35).string('fecha_prog_aforo').style(estilo_cabecera).style(celda_medio);
            hoja_1.cell(1,36).string('fecha_real_aforo').style(estilo_cabecera).style(celda_medio);
            hoja_1.cell(1,37).string('fecha_real_eta').style(estilo_cabecera).style(celda_medio);
            hoja_1.cell(1,38).string('fecha_retiro_puerto').style(estilo_cabecera).style(celda_medio);
            hoja_1.cell(1,39).string('hora_retiro').style(estilo_cabecera).style(celda_medio);
            hoja_1.cell(1,40).string('fecha_desconsolidacion_pudahuel').style(estilo_cabecera).style(celda_medio);
            hoja_1.cell(1,41).string('hora_desconsolidacion').style(estilo_cabecera).style(celda_medio);
            hoja_1.cell(1,42).string('estado_finanzas').style(estilo_cabecera).style(celda_medio);
            hoja_1.cell(1,43).string('fecha_de_pago_registro_1').style(estilo_cabecera).style(celda_medio);
            hoja_1.cell(1,44).string('fecha_de_pago_registro_2').style(estilo_cabecera).style(celda_medio);
            hoja_1.cell(1,45).string('fecha_de_pago_registro_3').style(estilo_cabecera).style(celda_medio);
            hoja_1.cell(1,46).string('fecha_de_pago_registro_4').style(estilo_cabecera).style(celda_medio);
            hoja_1.cell(1,47).string('fecha_de_pago_registro_5').style(estilo_cabecera).style(celda_medio);
            hoja_1.cell(1,48).string('fecha_solicitud_despacho').style(estilo_cabecera).style(celda_medio);
            hoja_1.cell(1,49).string('fecha_prog_despacho').style(estilo_cabecera).style(celda_medio);
            hoja_1.cell(1,50).string('direccion_entrega').style(estilo_cabecera).style(celda_medio);
            hoja_1.cell(1,51).string('comuna').style(estilo_cabecera).style(celda_medio);
            hoja_1.cell(1,52).string('tipo_entrega').style(estilo_cabecera).style(celda_medio);
            hoja_1.cell(1,53).string('fecha_listo_para_entrega').style(estilo_cabecera).style(celda_medio);
            hoja_1.cell(1,54).string('fecha_entrega').style(estilo_cabecera).style(celda_medio);
            hoja_1.cell(1,55).string('estado_entrega').style(estilo_cabecera).style(celda_medio);
            hoja_1.cell(1,56).string('chofer').style(estilo_cabecera).style(celda_medio);
            hoja_1.cell(1,57).string('dias_libres').style(estilo_cabecera).style(celda_medio);
            hoja_1.cell(1,58).string('fecha_creacion_cliente').style(estilo_cabecera).style(celda_medio);
            hoja_1.cell(1,59).string('m3_consolidados_nc').style(estilo_cabecera).style(celda_derecha);
            hoja_1.cell(1,60).string('fecha_envio_nc').style(estilo_cabecera).style(celda_derecha);
            hoja_1.cell(1,61).string('responsable_entrega').style(estilo_cabecera).style(celda_derecha);
            

            
            for(var i=0; i<Reporte.rows.length; i++)
            {
                row++;
                col = 1;

                hoja_1.cell(row,col).string(''+Reporte.rows[i]['id_proveedor'].toString()).style(estilo_contenido_texto).style(celda_izquierda); col++;
                hoja_1.cell(row,col).string(''+Reporte.rows[i]['nombre_proveedor'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                hoja_1.cell(row,col).string(''+Reporte.rows[i]['fecha_creacion_proveedor'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                hoja_1.cell(row,col).string(''+Reporte.rows[i]['id_cliente'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                hoja_1.cell(row,col).string(''+Reporte.rows[i]['razon_social_cliente'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                hoja_1.cell(row,col).string(''+Reporte.rows[i]['ejecutivo_comercial'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                hoja_1.cell(row,col).string(''+Reporte.rows[i]['ejecutivo_cuenta'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                hoja_1.cell(row,col).string(''+Reporte.rows[i]['bultos_esperados'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                hoja_1.cell(row,col).string(''+Reporte.rows[i]['m3_esperados'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                hoja_1.cell(row,col).string(''+Reporte.rows[i]['peso_esperado'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                hoja_1.cell(row,col).string(''+Reporte.rows[i]['bultos_recepcionados'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                hoja_1.cell(row,col).string(''+Reporte.rows[i]['bodega_recepcion'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                hoja_1.cell(row,col).string(''+Reporte.rows[i]['fecha_ultima_carga_documentos'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                hoja_1.cell(row,col).string(Reporte.rows[i]['fecha_recepcion_1']==null ? '':''+Reporte.rows[i]['fecha_recepcion_1'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                hoja_1.cell(row,col).string(Reporte.rows[i]['fecha_recepcion_2']==null ? '':''+Reporte.rows[i]['fecha_recepcion_2'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                hoja_1.cell(row,col).string(Reporte.rows[i]['fecha_recepcion_3']==null ? '':''+Reporte.rows[i]['fecha_recepcion_3'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                hoja_1.cell(row,col).string(Reporte.rows[i]['fecha_recepcion_4']==null ? '':''+Reporte.rows[i]['fecha_recepcion_4'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                hoja_1.cell(row,col).string(Reporte.rows[i]['fecha_recepcion_5']==null ? '':''+Reporte.rows[i]['fecha_recepcion_5'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                console.log(19);
                hoja_1.cell(row,col).string(''+Reporte.rows[i]['fecha_de_creacion_del_consolidado'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                console.log(20);
                hoja_1.cell(row,col).string(''+Reporte.rows[i]['fecha_cierre_consolidado_comercial'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                console.log(21);
                hoja_1.cell(row,col).string(''+Reporte.rows[i]['id_consolidado_comercial'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                console.log(22);
                hoja_1.cell(row,col).string(''+Reporte.rows[i]['tracking_id'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                console.log(23);
                hoja_1.cell(row,col).string(''+Reporte.rows[i]['proforma_id'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                console.log(24);
                hoja_1.cell(row,col).string(''+Reporte.rows[i]['fecha_consolidado_contenedor'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                console.log(25);
                hoja_1.cell(row,col).string(''+Reporte.rows[i]['fecha_ingreso_datos_contenedor_nave_eta'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                console.log(26);
                hoja_1.cell(row,col).string(''+Reporte.rows[i]['n_contenedor'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                console.log(27);
                hoja_1.cell(row,col).string(''+Reporte.rows[i]['despacho_id'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                console.log(28);
                hoja_1.cell(row,col).string(''+Reporte.rows[i]['nombre_nave'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                console.log(29);
                hoja_1.cell(row,col).string(''+Reporte.rows[i]['etd_nave_asignada'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                console.log(30);
                hoja_1.cell(row,col).string(''+Reporte.rows[i]['fecha_nueva_etd_o_eta'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                console.log(31);
                hoja_1.cell(row,col).string(''+Reporte.rows[i]['eta'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                console.log(32);
                hoja_1.cell(row,col).string(''+Reporte.rows[i]['n_carpeta'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                console.log(33);
                hoja_1.cell(row,col).string(''+Reporte.rows[i]['fecha_real_etd'].toString()).style(estilo_contenido_texto).style(celda_derecha); col++;
                console.log(34);
                hoja_1.cell(row,col).string(Reporte.rows[i]['dias_libres_restantes']==null ? '':''+Reporte.rows[i]['dias_libres_restantes'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                console.log(35);
                hoja_1.cell(row,col).string(''+Reporte.rows[i]['fecha_publicacion_aforo'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                console.log(36);
                hoja_1.cell(row,col).string(''+Reporte.rows[i]['fecha_real_aforo'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                console.log(37);
                hoja_1.cell(row,col).string(''+Reporte.rows[i]['fecha_real_eta'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                console.log(38);
                hoja_1.cell(row,col).string(''+Reporte.rows[i]['fecha_retiro_puerto'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                console.log(39);
                hoja_1.cell(row,col).string(''+Reporte.rows[i]['hora_retiro'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                console.log(40);
                hoja_1.cell(row,col).string(Reporte.rows[i]['fecha_listo_entrega']==null ? '':''+Reporte.rows[i]['fecha_listo_entrega'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                console.log(41);
                hoja_1.cell(row,col).string(Reporte.rows[i]['hora_listo_entrega']==null ? '':''+Reporte.rows[i]['hora_listo_entrega'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                console.log(42);
                hoja_1.cell(row,col).string(''+Reporte.rows[i]['estado_finanzas'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                console.log(43);
                hoja_1.cell(row,col).string(Reporte.rows[i]['fecha_pago_1']==null ? '':''+Reporte.rows[i]['fecha_pago_1'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                console.log(44);
                hoja_1.cell(row,col).string(Reporte.rows[i]['fecha_pago_2']==null ? '':''+Reporte.rows[i]['fecha_pago_2'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                console.log(45);
                hoja_1.cell(row,col).string(Reporte.rows[i]['fecha_pago_3']==null ? '':''+Reporte.rows[i]['fecha_pago_3'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                console.log(46);
                hoja_1.cell(row,col).string(Reporte.rows[i]['fecha_pago_4']==null ? '':''+Reporte.rows[i]['fecha_pago_4'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                console.log(47);
                hoja_1.cell(row,col).string(Reporte.rows[i]['fecha_pago_5']==null ? '':''+Reporte.rows[i]['fecha_pago_5'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                console.log(48);
                hoja_1.cell(row,col).string(Reporte.rows[i]['fecha_solicitud_despacho']==null ? '':''+Reporte.rows[i]['fecha_solicitud_despacho'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                console.log(49);
                hoja_1.cell(row,col).string(Reporte.rows[i]['fecha_programada']==null ? '':''+Reporte.rows[i]['fecha_programada'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                console.log(50);
                hoja_1.cell(row,col).string(Reporte.rows[i]['fk_direccion_completa']==null ? '':''+Reporte.rows[i]['fk_direccion_completa'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                console.log(51);
                hoja_1.cell(row,col).string(Reporte.rows[i]['fk_comuna_nombre']==null ? '':''+Reporte.rows[i]['fk_comuna_nombre'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                console.log(52);
                hoja_1.cell(row,col).string(Reporte.rows[i]['tipo_entrega']==null ? '':''+Reporte.rows[i]['tipo_entrega'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                console.log(53);
                hoja_1.cell(row,col).string(Reporte.rows[i]['fecha_listo_entrega']==null ? '':''+Reporte.rows[i]['fecha_listo_entrega'].toString()).style(estilo_contenido_texto).style(celda_derecha); col++;
                console.log(54);
                console.log(56);
                hoja_1.cell(row,col).string(''+Reporte.rows[i]['fecha_entrega'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                console.log(57);
                hoja_1.cell(row,col).string(''+Reporte.rows[i]['estado_entrega'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                console.log(58);
                hoja_1.cell(row,col).string(''+Reporte.rows[i]['chofer'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                console.log(59);
                hoja_1.cell(row,col).string(Reporte.rows[i]['dias_libres']==null ? '':''+Reporte.rows[i]['dias_libres'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                console.log(60);
                hoja_1.cell(row,col).string(''+Reporte.rows[i]['fecha_creacion_cliente'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                console.log(61);
                hoja_1.cell(row,col).string(''+Reporte.rows[i]['m3_recibidos'].toString()).style(estilo_contenido_texto).style(celda_derecha); col++;
                console.log(62);
                hoja_1.cell(row,col).string(''+Reporte.rows[i]['fecha_envio_nc'].toString()).style(estilo_contenido_texto).style(celda_derecha); col++;
                console.log(63);
                hoja_1.cell(row,col).string(Reporte.rows[i]['responsable_entrega']==null ? '':''+Reporte.rows[i]['responsable_entrega'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                console.log(64);
            }

        console.log('\n.::.');
        console.log('\n.::.');
        console.log('\n.::.');
        console.log('\nGuardando excel ');


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
                console.log("\nERROR "+err);
            } else {
            }
        });
    }

    res.status(200).send({
        message: "REPORTE GENERADO CORRECTAMENTE",
        success:true,
    }); res.end(); res.connection.destroy();

} catch (error) {
    console.log("\nERROR AL GENERAR REPORTE GATILLO BY API"+error);
    await client.query(` update public.excel_despachos set mensaje_carga = concat(mensaje_carga,'\n\n03.- ERROR `+error+`') `);
    res.status(400).send({
        message: "ERROR AL GENERAR REPORTE",
        success:false,
    }); res.end(); res.connection.destroy();
}};

exports.ProcesarExcelGatillosCronJob1 = async (req, res) => {
    console.log("\n.::. ProcesarExcelGatillosCronJob");
    console.log("\n.::. ProcesarExcelGatillosCronJob");
    var shc_ProcesarExcelGatillos1 = require('node-schedule');
    shc_ProcesarExcelGatillos1.scheduleJob('5 8 * * *', () => {
        function_ProcesarExcelGatillos_CronJob1();
    });

    async function function_ProcesarExcelGatillos_CronJob1(){
        let result=await client.query(`SELECT id FROM public.tracking where estado>=0 and id>=13761 order by id asc`);

		if(result && result.rows && result.rows.length>0){
			for(i=0;i<result.rows.length;i++){
                console.log('\n\n BUSCO FECHAS DE RECEPCION PARA '+result.rows[i].id)
				let r=await client.query(`select
				distinct fecha_recepcion,
				to_char( fecha_recepcion, 'DD/MM/YYYY') as fecha_format
				from public.tracking_detalle
				where tracking_id=`+result.rows[i].id+` order by fecha_recepcion asc`);
				console.log(result.rows[i].id);
				if(r && r.rows && r.rows.length>0){
                    console.log('\n\n TOTAL FECHAS ENCONTRADAS '+r.rows.length)
					//if(r.rows.length<=5){
						for(x=0;x<r.rows.length;x++){
							if(x==0){
								await client.query(`UPDATE public.tracking SET fecha_recepcion_1='`+r.rows[x].fecha_format+`' where id=`+result.rows[i].id);
							}else if(x==1){
								await client.query(`UPDATE public.tracking SET fecha_recepcion_2='`+r.rows[x].fecha_format+`' where id=`+result.rows[i].id);
							}else if(x==2){
								await client.query(`UPDATE public.tracking SET fecha_recepcion_3='`+r.rows[x].fecha_format+`' where id=`+result.rows[i].id);
							}else if(x==3){
								await client.query(`UPDATE public.tracking SET fecha_recepcion_4='`+r.rows[x].fecha_format+`' where id=`+result.rows[i].id);
							}else if(x==4){
								await client.query(`UPDATE public.tracking SET fecha_recepcion_5='`+r.rows[x].fecha_format+`' where id=`+result.rows[i].id);
							}
						}
					/*}else if(r.rows.length>5){
						console.log('\nmas de 5:'+result.rows[i].id);
						for(x=0;x<4;x++){
							if(x==0){
								await client.query(`UPDATE public.tracking SET fecha_recepcion_1='`+r.rows[x].fecha_format+`' where id=`+result.rows[i].id);
							}else if(x==1){
								await client.query(`UPDATE public.tracking SET fecha_recepcion_2='`+r.rows[x].fecha_format+`' where id=`+result.rows[i].id);
							}else if(x==2){
								await client.query(`UPDATE public.tracking SET fecha_recepcion_3='`+r.rows[x].fecha_format+`' where id=`+result.rows[i].id);
							}else if(x==3){
								await client.query(`UPDATE public.tracking SET fecha_recepcion_4='`+r.rows[x].fecha_format+`' where id=`+result.rows[i].id);
							}	
						}
						await client.query(`UPDATE public.tracking SET fecha_recepcion_5='`+r.rows[r.rows.length-1].fecha_format+`' where id=`+result.rows[i].id);
					}*/
				}
				if(i==result.rows.length-1){
					console.log('\nultimo');
				}
			}
			console.log('\nterminado paso 1');
		}

        await updateDataSla00();
        var fecha_carga = moment().format("DD-MM-YYYY HH:mm");
        await client.query(`update public.excel_despachos set mensaje_carga ='01.- ARCHIVO CARGADO',fecha_carga='`+fecha_carga+`', link_archivo=''`);

        
        var ExistenDatos = await client.query(` select * FROM public.excel_despachos WHERE mensaje_carga='01.- ARCHIVO CARGADO'`);

        if(ExistenDatos && ExistenDatos.rows && ExistenDatos.rows.length>0)
        {

            await client.query(` update public.excel_despachos set mensaje_carga = concat(mensaje_carga,'\n\n02.- PROCESANDO DATOS PARA REPORTE') `);

            await client.query(` DELETE FROM public.sla_00_completo where estado_despacho!='ENTREGADO' `);

            console.log('\n.::.');
            console.log('\n.::.');
            console.log('\n.::.');
            console.log('\nInsertando el reporte ');
            await client.query(`
            INSERT INTO public.sla_00_completo(id_proveedor, nombre_proveedor, fecha_creacion_proveedor, id_cliente, razon_social_cliente, ejecutivo, bultos_esperados, m3_esperados, peso_esperado, bultos_recepcionados, bodega_recepcion, fecha_ultima_carga_documentos, fecha_ultima_recepcion, fecha_de_creacion_del_consolidado, fecha_cierre_consolidado_comercial, id_consolidado_comercial, tracking_id, proforma_id, fecha_consolidado_contenedor, fecha_ingreso_datos_contenedor_nave_eta, n_contenedor, despacho_id, nombre_nave, etd_nave_asignada, fecha_nueva_etd_o_eta, eta, n_carpeta, fecha_publicacion, aforo, fecha_aforo, fecha_retiro, hora_retiro, fecha_desconsolidacion_pudahuel, hora_desconsolidacion, estado_finanzas, fecha_de_pago, fecha_ingreso_direccion, fecha_programada, fecha_entrega_retiro, estado_despacho, dias_libres, fecha_creacion_cliente, m3_recibidos, consolidado_rapido) 
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

            , case 
            when tck.consolidado_rapido is false then 'NO' 
            when tck.consolidado_rapido is true then 'SI' 
            else '' end as es_consolidado_rapido

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
            , case 
            when tck.consolidado_rapido is false then 'NO' 
            when tck.consolidado_rapido is true then 'SI' 
            else '' end
            `);

            await client.query(`
            UPDATE tracking a SET estado_sla_00 = true WHERE estado_sla_00 is not true and EXISTS (SELECT FROM sla_00_completo b WHERE b.tracking_id::text = a.id::text and b.estado_despacho='ENTREGADO')
            `);

            await client.query(` update public.excel_despachos set mensaje_carga = concat(mensaje_carga,'\n\n03.- CAPTURADO DATOS PARA ARCHIVO DE REPORTE') `);
        }

    }
};

exports.ProcesarExcelGatillosCronJob2 = async (req, res) => {
    console.log("\n.::. ProcesarExcelGatillosCronJob");
    console.log("\n.::. ProcesarExcelGatillosCronJob");
    var shc_ProcesarExcelGatillos2 = require('node-schedule');
    shc_ProcesarExcelGatillos2.scheduleJob('30 8 * * *', () => {
        function_ProcesarExcelGatillos_CronJob2();
    });

    async function function_ProcesarExcelGatillos_CronJob2(){
        
        console.log('\n.::.');
        console.log('\n.::.');
        console.log('\n.::.');
        console.log('\nCapturando el porte');
        var Reporte = await client.query(`
        SELECT DISTINCT t.id as tracking_id,
        coalesce(s.id_proveedor,'') as id_proveedor
        , coalesce(s.nombre_proveedor,'') as nombre_proveedor

        , case when length(s.fecha_creacion_proveedor)>0 and s.fecha_creacion_proveedor!='' and s.fecha_creacion_proveedor is not null and s.fecha_creacion_proveedor!='#¡REF!' then
        coalesce(to_char(to_date(s.fecha_creacion_proveedor, 'DD/MM/YYYY'), 'DD/MM/YYYY'),'') else
        '' end as fecha_creacion_proveedor

        , coalesce(s.id_cliente,'') as id_cliente
        , coalesce(s.razon_social_cliente,'') as razon_social_cliente
        , coalesce(s.ejecutivo,'') as ejecutivo_comercial
        ,CASE
        WHEN c.fk_ejecutivocuenta IS NOT NULL THEN
        coalesce(CONCAT(u.nombre,' ',u.apellidos),'')
        ELSE
        '' 
        END as ejecutivo_cuenta
        , coalesce(s.bultos_esperados::text,'') as bultos_esperados
        , REPLACE(coalesce(s.m3_esperados::text,''), '.', ',') as m3_esperados
        , REPLACE(coalesce(s.m3_recibidos::text,''), '.', ',') as m3_recibidos
        , coalesce(s.peso_esperado::text,'') as peso_esperado
        , coalesce(s.bultos_recepcionados::text,'') as bultos_recepcionados
        , coalesce(s.bodega_recepcion,'') as bodega_recepcion

        , case when s.fecha_ultima_carga_documentos='OK' then s.fecha_ultima_carga_documentos
        when length(s.fecha_ultima_carga_documentos)>0 and s.fecha_ultima_carga_documentos!='' and s.fecha_ultima_carga_documentos is not null and s.fecha_ultima_carga_documentos!='#¡REF!' then
        coalesce(to_char(to_date(s.fecha_ultima_carga_documentos, 'DD/MM/YYYY'), 'DD/MM/YYYY'),'') else

        case when s.fecha_cierre_consolidado_comercial='OK' then s.fecha_cierre_consolidado_comercial
        when length(s.fecha_cierre_consolidado_comercial)>0 and s.fecha_cierre_consolidado_comercial!='' and s.fecha_cierre_consolidado_comercial is not null and s.fecha_cierre_consolidado_comercial!='#¡REF!' then
        coalesce(to_char(to_date(s.fecha_cierre_consolidado_comercial, 'DD/MM/YYYY'), 'DD/MM/YYYY'),'') else
        '' end

        end as fecha_ultima_carga_documentos

        , coalesce(s.fecha_aprobacion_documentos,'') as fecha_aprobacion_documentos

        , case when s.fecha_ultima_recepcion='OK' then s.fecha_ultima_recepcion
        when length(s.fecha_ultima_recepcion)>0 and s.fecha_ultima_recepcion!='' and s.fecha_ultima_recepcion is not null and s.fecha_ultima_recepcion!='#¡REF!' then
        coalesce(to_char(to_date(s.fecha_ultima_recepcion, 'YYYY/MM/DD'), 'DD/MM/YYYY'),'') else
        '' end as fecha_ultima_recepcion_registro_1

        , case when s.fecha_de_creacion_del_consolidado='OK' then s.fecha_de_creacion_del_consolidado
        when length(s.fecha_de_creacion_del_consolidado)>0 and s.fecha_de_creacion_del_consolidado!='' and s.fecha_de_creacion_del_consolidado is not null and s.fecha_de_creacion_del_consolidado!='#¡REF!' then
        coalesce(to_char(to_date(s.fecha_de_creacion_del_consolidado, 'DD/MM/YYYY'), 'DD/MM/YYYY'),'') else
        '' end as fecha_de_creacion_del_consolidado

        , case when s.fecha_cierre_consolidado_comercial='OK' then s.fecha_cierre_consolidado_comercial
        when length(s.fecha_cierre_consolidado_comercial)>0 and s.fecha_cierre_consolidado_comercial!='' and s.fecha_cierre_consolidado_comercial is not null and s.fecha_cierre_consolidado_comercial!='#¡REF!' then
        coalesce(to_char(to_date(s.fecha_cierre_consolidado_comercial, 'DD/MM/YYYY'), 'DD/MM/YYYY'),'') else
        '' end as fecha_cierre_consolidado_comercial

        , coalesce(s.id_consolidado_comercial,'') as id_consolidado_comercial
        , coalesce(s.proforma_id,'') as proforma_id

        , case when s.fecha_consolidado_contenedor='OK' then s.fecha_consolidado_contenedor
        when length(s.fecha_consolidado_contenedor)>0 and s.fecha_consolidado_contenedor!='' and s.fecha_consolidado_contenedor is not null and s.fecha_consolidado_contenedor!='#¡REF!' then
        coalesce(to_char(to_date(s.fecha_consolidado_contenedor, 'DD/MM/YYYY'), 'DD/MM/YYYY'),'') else
        '' end as fecha_consolidado_contenedor

        , case when s.fecha_ingreso_datos_contenedor_nave_eta='OK' then s.fecha_ingreso_datos_contenedor_nave_eta
        when length(s.fecha_ingreso_datos_contenedor_nave_eta)>0 and s.fecha_ingreso_datos_contenedor_nave_eta!='' and s.fecha_ingreso_datos_contenedor_nave_eta is not null and s.fecha_ingreso_datos_contenedor_nave_eta!='#¡REF!' then
        coalesce(to_char(to_date(s.fecha_ingreso_datos_contenedor_nave_eta, 'YYYY/MM/DD'), 'DD/MM/YYYY'),'') else
        '' end as fecha_ingreso_datos_contenedor_nave_eta

        , coalesce(s.n_contenedor,'') as n_contenedor
        , coalesce(s.despacho_id,'') as despacho_id
        , coalesce(s.nombre_nave,'') as nombre_nave

        , case when s.etd_nave_asignada='OK' then s.etd_nave_asignada
        when length(s.etd_nave_asignada)>0 and s.etd_nave_asignada!='' and s.etd_nave_asignada is not null and s.etd_nave_asignada!='#¡REF!' then
        coalesce(to_char(to_date(s.etd_nave_asignada, 'DD/MM/YYYY'), 'DD/MM/YYYY'),'') else
        '' end as etd_nave_asignada

        , case when s.fecha_nueva_etd_o_eta='OK' then s.fecha_nueva_etd_o_eta
        when length(s.fecha_nueva_etd_o_eta)>0 and s.fecha_nueva_etd_o_eta!='' and s.fecha_nueva_etd_o_eta is not null and s.fecha_nueva_etd_o_eta!='#¡REF!' then
        coalesce(to_char(to_date(s.fecha_nueva_etd_o_eta, 'DD/MM/YYYY'), 'DD/MM/YYYY'),'') else
        '' end as fecha_nueva_etd_o_eta

        , case when s.eta='OK' then s.eta
        when length(s.eta)>0 and s.eta!='' and s.eta is not null then
        coalesce(to_char(to_date(s.eta, 'DD/MM/YYYY'), 'DD/MM/YYYY'),'') else
        '' end as eta

        , coalesce(s.n_carpeta,'') as n_carpeta

        , coalesce(s.fecha_retiro_puerto,'') as fecha_retiro_puerto
        , coalesce(s.hora_retiro,'') as hora_retiro

        , case when s.fecha_desconsolidacion_pudahuel='OK' then s.fecha_desconsolidacion_pudahuel
        when length(s.fecha_desconsolidacion_pudahuel)>0 and s.fecha_desconsolidacion_pudahuel!='' and s.fecha_desconsolidacion_pudahuel is not null and s.fecha_desconsolidacion_pudahuel!='#¡REF!'then 
        coalesce(to_char(to_date(s.fecha_desconsolidacion_pudahuel, 'DD/MM/YYYY'), 'DD/MM/YYYY'),'') else
        '' end as fecha_desconsolidacion_pudahuel

        ,case when t.fk_proforma is not null then
        coalesce(to_char(cp.fecha_desconsolidado, 'DD/MM/YYYY'),'') else
        '' end as fecha_listo_entrega 
        
        ,case when t.fk_proforma is not null then
        coalesce(to_char(cp.fecha_desconsolidado, 'HH24:mm'),'') else
        '' end as hora_listo_entrega 
        
        , coalesce(s.hora_desconsolidacion,'') as hora_desconsolidacion
        , coalesce(s.estado_finanzas,'') as estado_finanzas

        , case when s.fecha_de_pago='OK' then s.fecha_de_pago
        when length(s.fecha_de_pago)>0 and s.fecha_de_pago!='' and s.fecha_de_pago is not null and s.fecha_de_pago!='#¡REF!' then
        coalesce(to_char(to_date(s.fecha_de_pago, 'DD/MM/YYYY'), 'DD/MM/YYYY'),'') else
        '' end as fecha_de_pago_registro_1

        , case when s.fecha_ingreso_direccion='OK' then s.fecha_ingreso_direccion
        when length(s.fecha_ingreso_direccion)>0 and s.fecha_ingreso_direccion!='' and s.fecha_ingreso_direccion is not null and s.fecha_ingreso_direccion!='#¡REF!' then
        coalesce(to_char(to_date(s.fecha_ingreso_direccion, 'DD/MM/YYYY'), 'DD/MM/YYYY'),'') else
        '' end as fecha_ingreso_direccion

        , case when s.fecha_entrega_retiro='OK' then s.fecha_entrega_retiro
        when length(s.fecha_entrega_retiro)>0 and s.fecha_entrega_retiro!='' and s.fecha_entrega_retiro is not null and s.fecha_entrega_retiro!='#¡REF!' then
        coalesce(to_char(to_date(s.fecha_entrega_retiro, 'DD/MM/YYYY'), 'DD/MM/YYYY'),'') else
        '' end as fecha_entrega
        , coalesce(s.chofer,'') as chofer
        , cnt.dias_libres

        , coalesce(s.fecha_creacion_cliente,'') as fecha_creacion_cliente
        , case when tdr.fk_registro_direccion is not null then 
        concat(dir.direccion,' ',dir.numero,', ',comunas.nombre,', ',region.nombre) else '' end 
        as fk_direccion_completa
        ,case when tdr.fk_registro_direccion is not null then comunas.nombre else '' end as fk_comuna_nombre
        ,region.id as fk_region
        ,case when tdr.fk_registro_direccion is not null then region.nombre else '' end as fk_region_nombre
        /*,case when te.fecha_programada is not null then coalesce(to_char(te.fecha_programada, 'DD/MM/YYYY'),'') else
        '' end as fecha_programada */
        ,(SELECT te.fecha_programada FROM tracking_entrega te where te.fk_tracking=t.id and te.estado=true order by te.id desc limit 1) as fecha_programada
        ,case when tdr.fecha is not null then coalesce(to_char(tdr.fecha, 'DD/MM/YYYY'),'') else
        '' end as fecha_solicitud_despacho 
        ,CASE 
                        WHEN tdr.fk_bodega is not null then 'RETIRO' 
                        WHEN tdr.empresa_ext_retiro is not null then 'RETIRA TRANS.EXTERNO' 
                        WHEN tdr.empresa_ext_despacho is not null then 'DESPACHO TRANS.EXTERNO' 
                        WHEN tdr.fk_registro_direccion is null and tdr.fk_bodega is null and tdr.empresa_ext_retiro is null and tdr.empresa_ext_despacho is null then 'SIN ESPECIFICAR'
                        WHEN tdr.fk_registro_direccion is not null and dir.fk_region!=12 and dir.fk_region is not null then 'REVISAR DESPACHO GRATUITO NO INCLUIDO'
                        WHEN tdr.fk_registro_direccion is not null and dir.fk_region=12 and dir.fk_region is not null and dir.fk_comuna in(49,50,51,53,57,59,61,62,64,65,66,69,71,72,76,82,88,91) then 'REVISAR DESPACHO GRATUITO NO INCLUIDO'
                        WHEN tdr.fk_registro_direccion is not null and dir.fk_region=12 and dir.fk_region is not null and dir.fk_comuna not in(49,50,51,53,57,59,61,62,64,65,66,69,71,72,76,82,88,91) then 'DESPACHO GRATIS INCLUIDO'
                        ELSE 'SIN ESPECIFICAR' END tipo_entrega
        /*,CASE WHEN te.estado_entrega=1 then 'ENTREGADO' WHEN te.estado_entrega=2 then 'PARCIAL' ELSE '' END as estado_entrega*/
        ,CASE WHEN (SELECT te.estado_entrega FROM tracking_entrega te where te.fk_tracking=t.id and te.estado=true order by te.id desc limit 1)=1 then 'ENTREGADO'
        WHEN (SELECT te.estado_entrega FROM tracking_entrega te where te.fk_tracking=t.id and te.estado=true order by te.id desc limit 1)=1 then 'PARCIAL'
        ELSE '' END estado_entrega
        ,case when t.fk_proforma is not null then
        coalesce(to_char(cp.fecha_salida_puerto, 'DD/MM/YYYY'),'') else
        '' end as fecha_real_etd
        ,case when t.fk_proforma is not null then
        coalesce(to_char(cp.fecha_retiro_puerto, 'DD/MM/YYYY'),'') else
        '' end as fecha_real_eta 
        ,case when t.fk_proforma is not null then
        coalesce(to_char(cp.fecha_prog_aforo, 'DD/MM/YYYY'),'') else
        '' end as fecha_publicacion_aforo 
        ,case when t.fk_proforma is not null then
        coalesce(to_char(cp.fecha_real_aforo, 'DD/MM/YYYY'),'') else
        '' end as fecha_real_aforo 
        ,case when t.fk_proforma is not null then
        coalesce(to_char(to_date(cp.fecha_tnm_retiro,'DD/MM/YYYY'), 'DD/MM/YYYY'),'') else
        '' end as fecha_tnm_retiro 
        ,case when t.fk_proforma is not null then
        coalesce(to_char(cp.fecha_retiro_puerto, 'DD/MM/YYYY'),'') else
        '' end as fecha_retiro_puerto 
        ,coalesce((SELECT coalesce(CONCAT(u2.nombre,' ',u2.apellidos),'') FROM usuario u2 inner join public.tracking_entrega te on u2.id=te.fk_usuario where te.fk_tracking=t.id and te.estado=true order by te.id desc limit 1),'') as responsable_entrega
        /*,CASE
        WHEN te.id IS NOT NULL THEN
        coalesce(CONCAT(u2.nombre,' ',u2.apellidos),'')
        ELSE
        null 
        END as responsable_entrega*/
        ,coalesce((select nc."m3" from public.notas_cobros nc inner join public.despachos d on d.id=nc.fk_despacho where t.fk_cliente=d.fk_cliente and t.fk_proforma=d.fk_proforma and d.estado=true order by d.id desc limit 1), 0) as m3
        /*
        ,case when nc."m3" is not null then
        nc."m3" else
        0 end as m3
        ,case when s.id_consolidado_comercial is not null and s.id_consolidado_comercial!='' then
        coalesce(to_char(d."createdAt", 'DD/MM/YYYY'),'') else
        '' end as fecha_envio_nc*/
        ,case when s.id_consolidado_comercial is not null and s.id_consolidado_comercial!='' then
        coalesce(to_char((select d."createdAt" from public.despachos d where t.fk_cliente=d.fk_cliente and t.fk_proforma=d.fk_proforma and d.estado=true order by d.id desc limit 1), 'DD/MM/YYYY'),'') else
        '' end as fecha_envio_nc
        ,case when t.fecha_recepcion_1 is not null then
        coalesce(to_char(to_date(t.fecha_recepcion_1, 'DD/MM/YYYY'), 'DD/MM/YYYY'),'') else
        '' end as fecha_recepcion_1
        ,case when t.fecha_recepcion_2 is not null then
        coalesce(to_char(to_date(t.fecha_recepcion_2, 'DD/MM/YYYY'), 'DD/MM/YYYY'),'') else
        '' end as fecha_recepcion_2
        ,case when t.fecha_recepcion_3 is not null then
        coalesce(to_char(to_date(t.fecha_recepcion_3, 'DD/MM/YYYY'), 'DD/MM/YYYY'),'') else
        '' end as fecha_recepcion_3
        ,case when t.fecha_recepcion_4 is not null then
        coalesce(to_char(to_date(t.fecha_recepcion_4, 'DD/MM/YYYY'), 'DD/MM/YYYY'),'') else
        '' end as fecha_recepcion_4
        ,case when t.fecha_recepcion_5 is not null then
        coalesce(to_char(to_date(t.fecha_recepcion_5, 'DD/MM/YYYY'), 'DD/MM/YYYY'),'') else
        '' end as fecha_recepcion_5
        /*,case when nc.fecha_pago_1 is not null then
        coalesce(to_char(to_date(nc.fecha_pago_1, 'DD/MM/YYYY'), 'DD/MM/YYYY'),'') else
        '' end as fecha_pago_1
        ,case when nc.fecha_pago_2 is not null then
        coalesce(to_char(to_date(nc.fecha_pago_2, 'DD/MM/YYYY'), 'DD/MM/YYYY'),'') else
        '' end as fecha_pago_2
        ,case when nc.fecha_pago_3 is not null then
        coalesce(to_char(to_date(nc.fecha_pago_3, 'DD/MM/YYYY'), 'DD/MM/YYYY'),'') else
        '' end as fecha_pago_3
        ,case when nc.fecha_pago_4 is not null then
        coalesce(to_char(to_date(nc.fecha_pago_4, 'DD/MM/YYYY'), 'DD/MM/YYYY'),'') else
        '' end as fecha_pago_4
        ,case when nc.fecha_pago_5 is not null then
        coalesce(to_char(to_date(nc.fecha_pago_5, 'DD/MM/YYYY'), 'DD/MM/YYYY'),'') else
        '' end as fecha_pago_5*/
        ,
        DATE_PART('day',(cp.fecha_retiro_puerto + cnt.dias_libres * interval '1 day' - CURRENT_TIMESTAMP))  as dias_libres_restantes
        , coalesce(t.fecha_pago_1,'') as fecha_pago_1
        , coalesce(t.fecha_pago_2,'') as fecha_pago_2
        , coalesce(t.fecha_pago_3,'') as fecha_pago_3
        , coalesce(t.fecha_pago_4,'') as fecha_pago_4
        , coalesce(t.fecha_pago_5,'') as fecha_pago_5
        , coalesce(s.consolidado_rapido,'') as consolidado_rapido
        
        FROM public.sla_00_completo s
        INNER JOIN public.clientes c on c.id=s.id_cliente::integer
        LEFT JOIN public.usuario u on u.id=c.fk_ejecutivocuenta
        LEFT JOIN public.tracking_despacho_retiro tdr on tdr.fk_tracking=s.tracking_id::integer and tdr.estado=true
        LEFT JOIN public.clientes_direcciones as dir on dir.id=tdr.fk_registro_direccion
        LEFT JOIN public.comunas on comunas.id=dir.fk_comuna
        LEFT JOIN public.region on region.id=dir.fk_region
        LEFT JOIN public.tracking t on t.id=s.tracking_id::integer
        LEFT JOIN public.contenedor_proforma cp on cp.id=t.fk_proforma
        LEFT JOIN public.contenedor cnt on cnt.id=cp.fk_contenedor
        /*LEFT JOIN public.tracking_entrega te on te.fk_tracking=t.id::integer and te.estado=true*/

        /*LEFT JOIN public.usuario u2 on u2.id=te.fk_usuario */
        LEFT JOIN public.consolidado cns on cns.id=s.id_consolidado_comercial::integer
        /*LEFT JOIN public.despachos d on d.fk_cliente=t.fk_cliente and d.fk_proforma=t.fk_proforma and d.estado=true
        LEFT JOIN public.notas_cobros nc on nc.fk_despacho=d.id and nc.estado=true order by t.id asc*/
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
            hoja_1.cell(1,1).string('id_proveedor').style(estilo_cabecera).style(celda_izquierda);
            hoja_1.cell(1,2).string('nombre_proveedor').style(estilo_cabecera).style(celda_medio);
            hoja_1.cell(1,3).string('fecha_creacion_proveedor').style(estilo_cabecera).style(celda_medio);
            hoja_1.cell(1,4).string('id_cliente').style(estilo_cabecera).style(celda_medio);
            hoja_1.cell(1,5).string('razon_social_cliente').style(estilo_cabecera).style(celda_medio);
            hoja_1.cell(1,6).string('ejecutivo_comercial').style(estilo_cabecera).style(celda_medio);
            hoja_1.cell(1,7).string('ejecutivo_cuenta').style(estilo_cabecera).style(celda_medio);
            hoja_1.cell(1,8).string('bultos_esperados').style(estilo_cabecera).style(celda_medio);
            hoja_1.cell(1,9).string('m3_esperados').style(estilo_cabecera).style(celda_medio);
            hoja_1.cell(1,10).string('peso_esperado').style(estilo_cabecera).style(celda_medio);
            hoja_1.cell(1,11).string('bultos_recepcionados').style(estilo_cabecera).style(celda_medio);
            hoja_1.cell(1,12).string('bodega_recepcion').style(estilo_cabecera).style(celda_medio);
            hoja_1.cell(1,13).string('fecha_ultima_carga_documentos').style(estilo_cabecera).style(celda_medio);
            hoja_1.cell(1,14).string('fecha_ultima_recepcion_registro_1').style(estilo_cabecera).style(celda_medio);
            hoja_1.cell(1,15).string('fecha_ultima_recepcion_registro_2').style(estilo_cabecera).style(celda_medio);
            hoja_1.cell(1,16).string('fecha_ultima_recepcion_registro_3').style(estilo_cabecera).style(celda_medio);
            hoja_1.cell(1,17).string('fecha_ultima_recepcion_registro_4').style(estilo_cabecera).style(celda_medio);
            hoja_1.cell(1,18).string('fecha_ultima_recepcion_registro_5').style(estilo_cabecera).style(celda_medio);
            hoja_1.cell(1,19).string('fecha_de_creacion_del_consolidado').style(estilo_cabecera).style(celda_medio);
            hoja_1.cell(1,20).string('fecha_cierre_consolidado_comercial').style(estilo_cabecera).style(celda_medio);
            hoja_1.cell(1,21).string('id_consolidado_comercial').style(estilo_cabecera).style(celda_medio);
            hoja_1.cell(1,22).string('tracking_id').style(estilo_cabecera).style(celda_medio);
            hoja_1.cell(1,23).string('proforma_id').style(estilo_cabecera).style(celda_medio);
            hoja_1.cell(1,24).string('fecha_consolidado_contenedor').style(estilo_cabecera).style(celda_medio);
            hoja_1.cell(1,25).string('fecha_ingreso_datos_contenedor_nave_eta').style(estilo_cabecera).style(celda_medio);
            hoja_1.cell(1,26).string('n_contenedor').style(estilo_cabecera).style(celda_medio);
            hoja_1.cell(1,27).string('despacho_id').style(estilo_cabecera).style(celda_medio);
            hoja_1.cell(1,28).string('nombre_nave').style(estilo_cabecera).style(celda_medio);
            hoja_1.cell(1,29).string('etd_nave_asignada').style(estilo_cabecera).style(celda_medio);
            hoja_1.cell(1,30).string('fecha_nueva_etd_o_eta').style(estilo_cabecera).style(celda_medio);
            hoja_1.cell(1,31).string('eta').style(estilo_cabecera).style(celda_medio);
            hoja_1.cell(1,32).string('n_carpeta').style(estilo_cabecera).style(celda_medio);
            hoja_1.cell(1,33).string('fecha_real_etd').style(estilo_cabecera).style(celda_derecha);
            hoja_1.cell(1,34).string('dias_libres_restantes').style(estilo_cabecera).style(celda_derecha);
            hoja_1.cell(1,35).string('fecha_prog_aforo').style(estilo_cabecera).style(celda_medio);
            hoja_1.cell(1,36).string('fecha_real_aforo').style(estilo_cabecera).style(celda_medio);
            hoja_1.cell(1,37).string('fecha_real_eta').style(estilo_cabecera).style(celda_medio);
            hoja_1.cell(1,38).string('fecha_retiro_puerto').style(estilo_cabecera).style(celda_medio);
            hoja_1.cell(1,39).string('hora_retiro').style(estilo_cabecera).style(celda_medio);
            hoja_1.cell(1,40).string('fecha_desconsolidacion_pudahuel').style(estilo_cabecera).style(celda_medio);
            hoja_1.cell(1,41).string('hora_desconsolidacion').style(estilo_cabecera).style(celda_medio);
            hoja_1.cell(1,42).string('estado_finanzas').style(estilo_cabecera).style(celda_medio);
            hoja_1.cell(1,43).string('fecha_de_pago_registro_1').style(estilo_cabecera).style(celda_medio);
            hoja_1.cell(1,44).string('fecha_de_pago_registro_2').style(estilo_cabecera).style(celda_medio);
            hoja_1.cell(1,45).string('fecha_de_pago_registro_3').style(estilo_cabecera).style(celda_medio);
            hoja_1.cell(1,46).string('fecha_de_pago_registro_4').style(estilo_cabecera).style(celda_medio);
            hoja_1.cell(1,47).string('fecha_de_pago_registro_5').style(estilo_cabecera).style(celda_medio);
            hoja_1.cell(1,48).string('fecha_solicitud_despacho').style(estilo_cabecera).style(celda_medio);
            hoja_1.cell(1,49).string('fecha_prog_despacho').style(estilo_cabecera).style(celda_medio);
            hoja_1.cell(1,50).string('direccion_entrega').style(estilo_cabecera).style(celda_medio);
            hoja_1.cell(1,51).string('comuna').style(estilo_cabecera).style(celda_medio);
            hoja_1.cell(1,52).string('tipo_entrega').style(estilo_cabecera).style(celda_medio);
            hoja_1.cell(1,53).string('fecha_listo_para_entrega').style(estilo_cabecera).style(celda_medio);
            hoja_1.cell(1,54).string('fecha_entrega').style(estilo_cabecera).style(celda_medio);
            hoja_1.cell(1,55).string('estado_entrega').style(estilo_cabecera).style(celda_medio);
            hoja_1.cell(1,56).string('chofer').style(estilo_cabecera).style(celda_medio);
            hoja_1.cell(1,57).string('dias_libres').style(estilo_cabecera).style(celda_medio);
            hoja_1.cell(1,58).string('fecha_creacion_cliente').style(estilo_cabecera).style(celda_medio);
            hoja_1.cell(1,59).string('m3_consolidados_nc').style(estilo_cabecera).style(celda_derecha);
            hoja_1.cell(1,60).string('fecha_envio_nc').style(estilo_cabecera).style(celda_derecha);
            hoja_1.cell(1,61).string('responsable_entrega').style(estilo_cabecera).style(celda_derecha);
            hoja_1.cell(1,62).string('consolidado_rapido').style(estilo_cabecera).style(celda_derecha);
            
            for(var i=0; i<Reporte.rows.length; i++)
            {
                row++;
                col = 1;

                hoja_1.cell(row,col).string(''+Reporte.rows[i]['id_proveedor'].toString()).style(estilo_contenido_texto).style(celda_izquierda); col++;
                hoja_1.cell(row,col).string(''+Reporte.rows[i]['nombre_proveedor'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                hoja_1.cell(row,col).string(''+Reporte.rows[i]['fecha_creacion_proveedor'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                hoja_1.cell(row,col).string(''+Reporte.rows[i]['id_cliente'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                hoja_1.cell(row,col).string(''+Reporte.rows[i]['razon_social_cliente'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                hoja_1.cell(row,col).string(''+Reporte.rows[i]['ejecutivo_comercial'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                hoja_1.cell(row,col).string(''+Reporte.rows[i]['ejecutivo_cuenta'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                hoja_1.cell(row,col).string(''+Reporte.rows[i]['bultos_esperados'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                hoja_1.cell(row,col).string(''+Reporte.rows[i]['m3_esperados'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                hoja_1.cell(row,col).string(''+Reporte.rows[i]['peso_esperado'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                hoja_1.cell(row,col).string(''+Reporte.rows[i]['bultos_recepcionados'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                hoja_1.cell(row,col).string(''+Reporte.rows[i]['bodega_recepcion'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                hoja_1.cell(row,col).string(''+Reporte.rows[i]['fecha_ultima_carga_documentos'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                hoja_1.cell(row,col).string(Reporte.rows[i]['fecha_recepcion_1']==null ? '':''+Reporte.rows[i]['fecha_recepcion_1'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                hoja_1.cell(row,col).string(Reporte.rows[i]['fecha_recepcion_2']==null ? '':''+Reporte.rows[i]['fecha_recepcion_2'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                hoja_1.cell(row,col).string(Reporte.rows[i]['fecha_recepcion_3']==null ? '':''+Reporte.rows[i]['fecha_recepcion_3'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                hoja_1.cell(row,col).string(Reporte.rows[i]['fecha_recepcion_4']==null ? '':''+Reporte.rows[i]['fecha_recepcion_4'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                hoja_1.cell(row,col).string(Reporte.rows[i]['fecha_recepcion_5']==null ? '':''+Reporte.rows[i]['fecha_recepcion_5'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                console.log(19);
                hoja_1.cell(row,col).string(''+Reporte.rows[i]['fecha_de_creacion_del_consolidado'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                console.log(20);
                hoja_1.cell(row,col).string(''+Reporte.rows[i]['fecha_cierre_consolidado_comercial'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                console.log(21);
                hoja_1.cell(row,col).string(''+Reporte.rows[i]['id_consolidado_comercial'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                console.log(22);
                hoja_1.cell(row,col).string(''+Reporte.rows[i]['tracking_id'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                console.log(23);
                hoja_1.cell(row,col).string(''+Reporte.rows[i]['proforma_id'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                console.log(24);
                hoja_1.cell(row,col).string(''+Reporte.rows[i]['fecha_consolidado_contenedor'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                console.log(25);
                hoja_1.cell(row,col).string(''+Reporte.rows[i]['fecha_ingreso_datos_contenedor_nave_eta'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                console.log(26);
                hoja_1.cell(row,col).string(''+Reporte.rows[i]['n_contenedor'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                console.log(27);
                hoja_1.cell(row,col).string(''+Reporte.rows[i]['despacho_id'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                console.log(28);
                hoja_1.cell(row,col).string(''+Reporte.rows[i]['nombre_nave'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                console.log(29);
                hoja_1.cell(row,col).string(''+Reporte.rows[i]['etd_nave_asignada'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                console.log(30);
                hoja_1.cell(row,col).string(''+Reporte.rows[i]['fecha_nueva_etd_o_eta'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                console.log(31);
                hoja_1.cell(row,col).string(''+Reporte.rows[i]['eta'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                console.log(32);
                hoja_1.cell(row,col).string(''+Reporte.rows[i]['n_carpeta'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                console.log(33);
                hoja_1.cell(row,col).string(''+Reporte.rows[i]['fecha_real_etd'].toString()).style(estilo_contenido_texto).style(celda_derecha); col++;
                console.log(34);
                hoja_1.cell(row,col).string(Reporte.rows[i]['dias_libres_restantes']==null ? '':''+Reporte.rows[i]['dias_libres_restantes'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                console.log(35);
                hoja_1.cell(row,col).string(''+Reporte.rows[i]['fecha_publicacion_aforo'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                console.log(36);
                hoja_1.cell(row,col).string(''+Reporte.rows[i]['fecha_real_aforo'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                console.log(37);
                hoja_1.cell(row,col).string(''+Reporte.rows[i]['fecha_real_eta'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                console.log(38);
                hoja_1.cell(row,col).string(''+Reporte.rows[i]['fecha_retiro_puerto'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                console.log(39);
                hoja_1.cell(row,col).string(''+Reporte.rows[i]['hora_retiro'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                console.log(40);
                hoja_1.cell(row,col).string(Reporte.rows[i]['fecha_listo_entrega']==null ? '':''+Reporte.rows[i]['fecha_listo_entrega'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                console.log(41);
                hoja_1.cell(row,col).string(Reporte.rows[i]['hora_listo_entrega']==null ? '':''+Reporte.rows[i]['hora_listo_entrega'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                console.log(42);
                hoja_1.cell(row,col).string(''+Reporte.rows[i]['estado_finanzas'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                console.log(43);
                hoja_1.cell(row,col).string(Reporte.rows[i]['fecha_pago_1']==null ? '':''+Reporte.rows[i]['fecha_pago_1'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                console.log(44);
                hoja_1.cell(row,col).string(Reporte.rows[i]['fecha_pago_2']==null ? '':''+Reporte.rows[i]['fecha_pago_2'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                console.log(45);
                hoja_1.cell(row,col).string(Reporte.rows[i]['fecha_pago_3']==null ? '':''+Reporte.rows[i]['fecha_pago_3'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                console.log(46);
                hoja_1.cell(row,col).string(Reporte.rows[i]['fecha_pago_4']==null ? '':''+Reporte.rows[i]['fecha_pago_4'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                console.log(47);
                hoja_1.cell(row,col).string(Reporte.rows[i]['fecha_pago_5']==null ? '':''+Reporte.rows[i]['fecha_pago_5'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                console.log(48);
                hoja_1.cell(row,col).string(Reporte.rows[i]['fecha_solicitud_despacho']==null ? '':''+Reporte.rows[i]['fecha_solicitud_despacho'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                console.log(49);
                hoja_1.cell(row,col).string(Reporte.rows[i]['fecha_programada']==null ? '':''+Reporte.rows[i]['fecha_programada'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                console.log(50);
                hoja_1.cell(row,col).string(Reporte.rows[i]['fk_direccion_completa']==null ? '':''+Reporte.rows[i]['fk_direccion_completa'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                console.log(51);
                hoja_1.cell(row,col).string(Reporte.rows[i]['fk_comuna_nombre']==null ? '':''+Reporte.rows[i]['fk_comuna_nombre'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                console.log(52);
                hoja_1.cell(row,col).string(Reporte.rows[i]['tipo_entrega']==null ? '':''+Reporte.rows[i]['tipo_entrega'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                console.log(53);
                hoja_1.cell(row,col).string(Reporte.rows[i]['fecha_listo_entrega']==null ? '':''+Reporte.rows[i]['fecha_listo_entrega'].toString()).style(estilo_contenido_texto).style(celda_derecha); col++;
                console.log(54);
                console.log(56);
                hoja_1.cell(row,col).string(''+Reporte.rows[i]['fecha_entrega'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                console.log(57);
                hoja_1.cell(row,col).string(''+Reporte.rows[i]['estado_entrega'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                console.log(58);
                hoja_1.cell(row,col).string(''+Reporte.rows[i]['chofer'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                console.log(59);
                hoja_1.cell(row,col).string(Reporte.rows[i]['dias_libres']==null ? '':''+Reporte.rows[i]['dias_libres'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                console.log(60);
                hoja_1.cell(row,col).string(''+Reporte.rows[i]['fecha_creacion_cliente'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                console.log(61);
                hoja_1.cell(row,col).string(''+Reporte.rows[i]['m3_recibidos'].toString()).style(estilo_contenido_texto).style(celda_derecha); col++;
                console.log(62);
                hoja_1.cell(row,col).string(''+Reporte.rows[i]['fecha_envio_nc'].toString()).style(estilo_contenido_texto).style(celda_derecha); col++;
                console.log(63);
                hoja_1.cell(row,col).string(Reporte.rows[i]['responsable_entrega']==null ? '':''+Reporte.rows[i]['responsable_entrega'].toString()).style(estilo_contenido_texto).style(celda_medio); col++;
                console.log(64);
                hoja_1.cell(row,col).string(Reporte.rows[i]['consolidado_rapido']==null ? '':''+Reporte.rows[i]['consolidado_rapido'].toString()).style(estilo_contenido_texto).style(celda_derecha); col++;
                console.log(65);
            }

        } catch (error) {
            await client.query(` update public.excel_despachos set mensaje_carga = concat(mensaje_carga,'\n\n03.- ERROR `+error+`') `);
            console.log("\nERROR "+error);
            res.status(400).send({
                message: "ERROR AL CREAR EXCEL",
                success:false,
            }); res.end(); res.connection.destroy();
        }

        console.log('\n.::.');
        console.log('\n.::.');
        console.log('\n.::.');
        console.log('\nGuardando excel ');


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
                console.log("\nERROR "+err);
            } else {
            }
        });

        /* ENVIANDO REPORTE POR CORREO */
        /* ENVIANDO REPORTE POR CORREO */
        console.log('\n\n INICIO ENVIANDO ARCHIVO REPORTE GATILLOS '+moment().format("DD-MM-YYYY HH:mm"));
        var Correos = await client.query(`
        SELECT 
        emails
        FROM public.reportes_email
        where
        id=3
        `);
        await enviarEmail.mail_reporte_gatillos({
            asunto:'REPORTE GATILLOS '+moment().format("DD-MM-YYYY HH:mm"),
            email:Correos.rows[0]['emails'].split(";"),
        });
        console.log('\n\n FIN ENVIANDO ARCHIVO REPORTE GATILLOS '+moment().format("DD-MM-YYYY HH:mm"));
        /* ENVIANDO REPORTE POR CORREO */
        /* ENVIANDO REPORTE POR CORREO */
    };
};

exports.CrearEnviar_ReporteMasterBD_New = async (req, res) => {

    console.log("\n.::. MASTER BD NEW");
    var shc_CrearEnviar_ReporteMasterBD_New = require('node-schedule');
    shc_CrearEnviar_ReporteMasterBD_New.scheduleJob('0 10 * * *', () => {
        FUNCT_CrearEnviar_ReporteMasterBD_New();
    });

    async function FUNCT_CrearEnviar_ReporteMasterBD_New(){

        /* PRIMERO ELIMINAMOS EL ARCHIVO ACTUAL */
        /* PRIMERO ELIMINAMOS EL ARCHIVO ACTUAL */
        const filePath = './public/files/Reporte_Master_BD.xlsx';
        console.log('\nESTE ES EL ARCHIVO '+filePath)
        await fs.unlink(filePath, (err) => {
          if (err) {
            console.error('Error al eliminar el archivo:', err);
            return;
          }
          console.log('\nArchivo eliminado exitosamente');
        });
        /* PRIMERO ELIMINAMOS EL ARCHIVO ACTUAL */
        /* PRIMERO ELIMINAMOS EL ARCHIVO ACTUAL */

        /* SEGUNDO CALCULO DEL REPORTE */
        /* SEGUNDO CALCULO DEL REPORTE */
        console.log('\nREPORTE MASTER BD 1');
        var MesesCerrados = await client.query(`
        SELECT 
        ing_fk_responsable
        , ing_fecha
        , del_fk_responsable
        , del_fecha
        , id
        , anio
        , mes
        FROM public.master_bd_mes_cerrado
        where
        del_fk_responsable=0
        `);
    
        var CondicioDelete = '';
        var CondicionSelect = '';
    
        console.log('\n\n CANTIDAD DE MESES CERRADOS '+JSON.stringify(MesesCerrados));
    
        if(MesesCerrados.rows.length>0)
        {
            for(var i=0; i<MesesCerrados.rows.length; i++) 
            {
                if(i==0)
                {
                    CondicioDelete = ` where substring(n_carpeta,2,4)!='`+MesesCerrados.rows[i].anio.substring(2,4)+``+MesesCerrados.rows[i].mes+`' `;
                }
                else
                {
                    CondicioDelete += ` and substring(n_carpeta,2,4)!='`+MesesCerrados.rows[i].anio.substring(2,4)+``+MesesCerrados.rows[i].mes+`' `;
                }
    
                CondicionSelect += ` and substring(d.n_carpeta,2,4)!='`+MesesCerrados.rows[i].anio.substring(2,4)+``+MesesCerrados.rows[i].mes+`' `;
            }
        }
    
        console.log('\n\n CONDICION SELECT '+JSON.stringify(CondicionSelect));
    
        await client.query(` delete from public.master_bd `+CondicioDelete+` `);

        console.log('\n\n INICIO CALCULO MASTER BD '+moment().format("DD-MM-YYYY HH:mm"));
    
        var QuerySelectMasterBd = `
        SELECT
        DISTINCT
        pro."createdAt" as fecha_creacion
        , coalesce(pro.id,0) as nc_id
        , coalesce(d.n_carpeta,'') as N_CARPETA
        , CASE WHEN pro.fk_despacho=-1 THEN coalesce(d.rut,'') ELSE coalesce(c.rut,'') END as RUT
        , coalesce(d.contenedor,'') as CONTENEDOR
        , coalesce(d.fk_nave, 0) as ID_NAVE
        , coalesce(d.nave_nombre, '[No ingresado]') as NOMBRE_NAVE
        , coalesce(to_char(d.eta, 'DD-MM-YYYY'),'') as ETA
        , ROUND(coalesce(pro.carga,0), 2) as M3
        , round( ROUND(((coalesce(pro.cif, 0)+coalesce(pro.arancel, 0))*19/100)+coalesce(pro.arancel, 0)+coalesce(pro.impto_port, 0), 2) * coalesce(pro.tc,0)) as MONTO_DIN
        , round( ROUND(((coalesce(pro.cif, 0)+coalesce(pro.arancel, 0))*19/100)+coalesce(pro.arancel, 0)+coalesce(pro.impto_port, 0), 2) * coalesce(pro.tc,0)) + coalesce(pro.ajuste_m_1, 0) as MONTO_DIN_AJUSTADO
        , ROUND(coalesce(pro.carga, 0)*coalesce(pro.costo, 0)+coalesce(pro.pb, 0), 2) as MONTO_CARGA_USD
        , ROUND((coalesce(pro.carga, 0)*coalesce(pro.costo, 0)+coalesce(pro.pb, 0))*coalesce(pro.tc2, 0)) as MONTO_CARGA_CLP
        , ROUND((coalesce(pro.carga, 0)*coalesce(pro.costo, 0)+coalesce(pro.pb, 0))*coalesce(pro.tc2, 0))+coalesce(pro.ajuste_m_2, 0) as MONTO_CARGA_CLP_AJUSTADO
        , ROUND(coalesce(pro.aforo, 0) + coalesce(pro.isp, 0) + coalesce(pro.cda, 0) + coalesce(pro.almacenaje, 0) + ( coalesce(pro.pallet_valor_u, 0) * coalesce(pro.pallet, 0)) + coalesce(pro.ttvp, 0) + coalesce(pro.twsc, 0) + coalesce(pro.otros, 0) + coalesce(pro.ajuste_m_2, 0) + coalesce(pro.ajuste_m_1, 0)) as TOTAL_GASTOS
        , ROUND (
        round( ROUND(((coalesce(pro.cif, 0)+coalesce(pro.arancel, 0))*19/100)+coalesce(pro.arancel, 0)+coalesce(pro.impto_port, 0), 2) * coalesce(pro.tc,0)) + coalesce(pro.ajuste_m_1, 0)
        + ROUND(coalesce(pro.aforo, 0) + coalesce(pro.isp, 0) + coalesce(pro.cda, 0) + coalesce(pro.almacenaje, 0) + ( coalesce(pro.pallet_valor_u, 0) * coalesce(pro.pallet, 0)) + coalesce(pro.ttvp, 0) + coalesce(pro.twsc, 0) + coalesce(pro.otros, 0) + coalesce(pro.ajuste_m_2, 0) + coalesce(pro.ajuste_m_1, 0))
        + ( coalesce(pro.tc2, 0) *
        (
        coalesce(pro.pb, 0) + coalesce(pro.carga, 0) * coalesce(pro.excento, 0)
        + ( ( coalesce(pro.carga, 0) * coalesce(pro.afecto, 0) ) + ( ( coalesce(pro.carga, 0) * coalesce(pro.afecto, 0) ) *0.19 ) )
        ) )
        ) as TOTAL_PROVISION
        , (
        ROUND (
        round( ROUND(((coalesce(pro.cif, 0)+coalesce(pro.arancel, 0))*19/100)+coalesce(pro.arancel, 0)+coalesce(pro.impto_port, 0), 2) * coalesce(pro.tc,0)) + coalesce(pro.ajuste_m_1, 0)
        + ROUND(coalesce(pro.aforo, 0) + coalesce(pro.isp, 0) + coalesce(pro.cda, 0) + coalesce(pro.almacenaje, 0) + ( coalesce(pro.pallet_valor_u, 0) * coalesce(pro.pallet, 0)) + coalesce(pro.ttvp, 0) + coalesce(pro.twsc, 0) + coalesce(pro.otros, 0) + coalesce(pro.ajuste_m_2, 0) + coalesce(pro.ajuste_m_1, 0))
        + ( coalesce(pro.tc2, 0) *
        (
        coalesce(pro.pb, 0) + coalesce(pro.carga, 0) * coalesce(pro.excento, 0)
        + ( ( coalesce(pro.carga, 0) * coalesce(pro.afecto, 0) ) + ( ( coalesce(pro.carga, 0) * coalesce(pro.afecto, 0) ) *0.19 ) )
        ) )
        )
        ) -
        (
        ROUND(coalesce((
            select
            SUM(case when cbc.conversion is null then cbc.debit_amt else (cbc.debit_amt*cbc.conversion) end)
            from public.wsc_envio_asientos_cabeceras cbc
            where
            cbc.carpeta=d.n_carpeta
            and cbc.estado<>'false'
            and cbc.estado<>'ELIMINADO'
            and coalesce(cbc.tipo_pago2,'')<>'COMISION'
            and coalesce(cbc.estado,'')<>'ERROR DUPLICADO'
            and cbc.com_text like '%Ingreso de pagos de clientes%'
            ), 0))
        ) as TOTAL_A_PAGAR
        , ROUND(coalesce((
            select
            SUM(case when cbc.conversion is null then cbc.debit_amt else (cbc.debit_amt*cbc.conversion) end)
            from public.wsc_envio_asientos_cabeceras cbc
            where
            cbc.carpeta=d.n_carpeta
            and cbc.estado<>'false'
            and cbc.estado<>'ELIMINADO'
            and coalesce(cbc.tipo_pago2,'')<>'COMISION'
            and coalesce(cbc.estado,'')<>'ERROR DUPLICADO'
            and cbc.com_text like '%Ingreso de pagos de clientes%'
            ), 0)) as MONTO_PAGADO
        , SUBSTRING(coalesce(d.n_carpeta,''), 2, 2) as ANO
        , SUBSTRING(coalesce(d.n_carpeta,''), 4, 2) as MES
        , SUBSTRING(coalesce(d.n_carpeta,''), 1, 7) as BASE
        , ROUND(coalesce(pro.aforo,0)) as AFORO
        , ROUND(coalesce(pro.cda,0)) as CDA
        , ROUND(coalesce(pro.isp,0)) as ISP
        , ROUND(coalesce(pro.pallet, 0)*coalesce(pro.pallet_valor_u, 0)) as PALLETS
        , ROUND(coalesce(pro.ttvp, 0)) as TVP
        , ROUND(coalesce(pro.twsc, 0)) as OTRO_TRANSPORTE
        , ROUND(coalesce(pro.otros, 0)) as otros
        , coalesce(pro.detalle_otro, '') as detalle_otro
        , ROUND(coalesce(pro.ajuste_m_1, 0)) as MONTO_AJU_DIN
        , coalesce(pro.ajuste_c_1, '') as DETALLE_AJU_DIN
        , ROUND(coalesce(pro.ajuste_m_2, 0)) as MONTO_AJU_SERV
        , coalesce(pro.ajuste_c_2, '') as DETALLE_AJU_SERV
        , ROUND(coalesce(pro.tc2,0), 2) as TC_SERVICIO
        , coalesce(pro.pb,0) as PRECIO_BASE
        , coalesce(pro.costo,0) as PRECIO_UNITARIO_X_M3
        , coalesce(ejec.nombre, '') as EJECUTIVO
        , coalesce(to_char(d.eta, 'MM'),'') as MES_ETA
        , coalesce(pro.din, 0) as din
        , coalesce(d.fk_cliente,0) as fk_cliente
        , coalesce(to_char(pro.din_ingresada_fecha, 'DD-MM-YYYY'), '') as din_ingresada_fecha
        , coalesce(c.codigo, '') as nombre_cliente
        , CASE WHEN pro.din_pagada_flag=true THEN 'SI' ELSE 'NO' END as din_pagada_flag
        , CASE WHEN pro.din_pagada_flag=true THEN coalesce(to_char(pro.din_pagada_fecha, 'DD-MM-YYYY'),'') ELSE '' END as din_pagada_fecha
        , (select coalesce(temp1.id,0) from public.consolidado as temp1 where temp1.n_carpeta=d.n_carpeta order by id desc limit 1) as fk_servicio
        , 'NO' as DOC_FACTURA
        , 'NO' as DOC_DIN
        , 'NO' as DOC_F_AGENCIA
        , 'NO' as DOC_TGR
        , ROUND(coalesce(pro.almacenaje,0)) as ALMACENAJE
        FROM public.notas_cobros pro
        INNER JOIN public.despachos d ON CASE WHEN pro.fk_despacho = -1 THEN pro.codigo_unificacion=d.codigo_unificacion and d.estado is true ELSE pro.fk_despacho = d.id and d.estado is true END
        LEFT JOIN public.clientes c ON c.id = d.fk_cliente and c.valido_reportes='SI'
        LEFT JOIN public.usuario as ejec ON ejec.id = c.fk_comercial
        where
        pro.estado<>false
        `+CondicionSelect+`
        `;

        console.log('\n\n QUERY SELECT MASTERBT \n\n'+QuerySelectMasterBd);

        await client.query(`
        insert into public.master_bd (fecha_creacion, nc_id, n_carpeta, rut, contenedor, id_nave, nombre_nave, eta, m3, monto_din, monto_din_ajuste, monto_carga_usd, monto_carga_clp, monto_carga_clp_ajuste, total_gastos, total_provision, total_a_pagar, monto_pagado, ano, mes, base, aforo, cda, isp, pallets, tvp, otro_transporte, otros, detalle_otro, monto_aju_din, detalle_aju_din, monto_aju_serv, detalle_aju_serv, tc_servicio, precio_base, precio_unitario_x_m3, ejecutivo, mes_eta, din, fk_cliente, din_ingresada_fecha, nombre_cliente, din_pagada_flag, din_pagada_fecha, fk_servicio, doc_factura, doc_din, doc_f_agencia, doc_tgr, almacenaje) 
        `+QuerySelectMasterBd+`
        `);

        console.log('\n\n FIN CALCULO MASTER BD '+moment().format("DD-MM-YYYY HH:mm"));
    
        var fecha_carga = moment().format("DD-MM-YYYY HH:mm");
    
        console.log(` update public.master_bd set ejecucion_fecha='`+fecha_carga+`', link_descarga='`+process.env.UrlRemoteServer1+`get_master_bd' `);
        await client.query(` update public.master_bd set ejecucion_fecha='`+fecha_carga+`', link_descarga='`+process.env.UrlRemoteServer1+`get_master_bd' `);
        /* SEGUNDO CALCULO DEL REPORTE */
        /* SEGUNDO CALCULO DEL REPORTE */


        /* CREO REPORTE EXCEL DEL REPORTE */
        /* CREO REPORTE EXCEL DEL REPORTE */
        console.log('\n\n INICIO CREAR EXCEL MASTER BD '+moment().format("DD-MM-YYYY HH:mm"));
        var Reporte = await client.query(` 
        SELECT 
        n_carpeta as "N° CARPETA"
        , rut as "RUT"
        , contenedor as "CONTENEDOR"
        , id_nave as "ID NAVE"
        , nombre_nave as "NOMBRE NAVE"
        , eta as "ETA"
        , m3 as "M3"
        , monto_din as "MONTO DIN"
        , monto_din_ajuste as "MONTO DIN AJUSTE"
        , monto_carga_usd as "MONTO CARGA USD"
        , monto_carga_clp as "MONTO CARGA CLP"
        , monto_carga_clp_ajuste as "MONTO CARGA CLP AJUSTE"
        , total_gastos as "TOTAL GASTOS"
        , total_provision as "TOTAL PROVISION"
        , total_a_pagar as "TOTAL A PAGAR"
        , monto_pagado as "MONTO PAGADO"
        , ano as "AÑO"
        , mes as "MES"
        , base as "BASE"
        , aforo as "AFORO"
        , cda as "CDA"
        , isp as "ISP"
        , pallets as "PALLETS"
        , tvp as "TVP"
        , otro_transporte as "OTRO TRANSPORTE"
        , otros as "OTROS"
        , detalle_otro as "DETALLE OTROS"
        , monto_aju_din as "MONTO AJUSTE DIN"
        , detalle_aju_din as "DETALLE AJUSTE DIN"
        , monto_aju_serv as "MONTO AJUSTE SERV"
        , detalle_aju_serv as "DETALLE AJUSTE SERV"
        , tc_servicio as "TC SERVICIO"
        , precio_base as "PRECIO BASE"
        , precio_unitario_x_m3 as "PRECIO UNITARIO XM3"
        , ejecutivo as "EJECUTIVO"
        , mes_eta as "MES ETA"
        , din as "N DIN"
        , fk_cliente as "ID CLIENTE"
        , nombre_cliente as "CODIGO CLIENTE"
        , din_pagada_flag as "DIN PAGADA TESORERIA"
        , din_pagada_fecha as "FECHA PAGO DIN"
        , din_ingresada_fecha as "FECHA INGRESO DIN"
        , fk_servicio as "ID SERVICIO"
        , 'NO' as "DOC FACTURA"
        , 'NO' as "DOC DIN"
        , 'NO' as "DOC F.AGENCIA"
        , 'NO' as "DOC TGR"

        FROM public.master_bd 
        `);
        var xl = require('excel4node');
        const workbook = new xl.Workbook();
        const worksheet = workbook.addWorksheet('Master_Bd');
        const columns = Object.keys(Reporte.rows[0]);

        const numberFormat = workbook.createStyle({
            numberFormat: '#,##0.00', // Usa el formato que necesites (por ejemplo, sin decimales: '#,##0')
          });


        columns.forEach((column, colIndex) => {
            worksheet.cell(1, colIndex + 1).string(column);
        });

        Reporte.rows.forEach((row, rowIndex) => {
            columns.forEach((column, colIndex) => {
                const value = row[column];
        
                // Verifica si value es nulo o undefined
                if (value !== null && value !== undefined) {
                    if (
                        colIndex === 7  || colIndex === 8   || colIndex === 9   ||
                        colIndex === 10 || colIndex === 11  || colIndex === 12  ||
                        colIndex === 13 || colIndex === 14  || colIndex === 15  ||
                        colIndex === 19 || colIndex === 20  || colIndex === 21  ||
                        colIndex === 22 || colIndex === 23  || colIndex === 24  ||
                        colIndex === 25 || colIndex === 27  || colIndex === 29  ||
                        colIndex === 31 || colIndex === 32  || colIndex === 33
                    ) 
                    {
                        // Asegúrate de convertir correctamente a número
                        const numberValue = Number(value.toString().trim()); // Usar trim para eliminar espacios
                        if (!isNaN(numberValue)) 
                        {
                            worksheet.cell(rowIndex + 2, colIndex + 1).number(numberValue).style(numberFormat); // Aplica el estilo
                        } 
                        else 
                        {
                            // En caso de no ser un número, guárdalo como cadena para depuración
                            worksheet.cell(rowIndex + 2, colIndex + 1).string(value.toString());
                        }
                    } 
                    else 
                    {
                        worksheet.cell(rowIndex + 2, colIndex + 1).string(value.toString());
                    }
                } else {
                    // Maneja el caso cuando value es null o undefined
                    console.log(`\n Valor nulo o indefinido en la columna ${colIndex}, fila ${rowIndex}`);
                    worksheet.cell(rowIndex + 2, colIndex + 1).string(''); // O maneja esto como necesites
                }
            });
        });
        
        workbook.write('./public/files/Reporte_Master_BD.xlsx', (err, stats) => {
            if (err) 
            {
                console.error('Error al guardar el archivo Excel:', err);
            } else {
                console.log('\nArchivo Excel guardado exitosamente:', filePath);
            }
        });
        console.log('\n\n FIN CREAR EXCEL MASTER BD '+moment().format("DD-MM-YYYY HH:mm"));
        /* CREO REPORTE EXCEL DEL REPORTE */
        /* CREO REPORTE EXCEL DEL REPORTE */
        
        
        
        console.log('\n\n FIN CREACION ARCHIVO MASTER BD '+moment().format("DD-MM-YYYY HH:mm"));
        /* CREO REPORTE EXCEL DEL REPORTE */
        /* CREO REPORTE EXCEL DEL REPORTE */        

        /* INICIO ENVIANDO REPORTE EXCEL DEL REPORTE */
        /* INICIO ENVIANDO REPORTE EXCEL DEL REPORTE */
        console.log('\n\n INICIO ENVIANDO ARCHIVO MASTER BD '+moment().format("DD-MM-YYYY HH:mm"));
        var Correos = await client.query(`
        SELECT 
        emails
        FROM public.reportes_email
        where
        id=1
        `);
        
        await enviarEmail.mail_reporte_masterbd({
            asunto:'REPORTE MASTER BD '+moment().format("DD-MM-YYYY HH:mm"),
            email:Correos.rows[0]['emails'].split(";"),
        });
        console.log('\n\n FIN ENVIANDO ARCHIVO MASTER BD '+moment().format("DD-MM-YYYY HH:mm"));
        /* INICIO ENVIANDO REPORTE EXCEL DEL REPORTE */
        /* INICIO ENVIANDO REPORTE EXCEL DEL REPORTE */
    }
};

exports.CrearEnviar_ReporteMasterBD = async (req, res) => {

    console.log("\n.::. MASTER BD");
    var shc_CrearEnviar_ReporteMasterBD = require('node-schedule');
    shc_CrearEnviar_ReporteMasterBD.scheduleJob('30 7 * * *', () => {
        FUNCT_CrearEnviar_ReporteMasterBD();
    });

    async function FUNCT_CrearEnviar_ReporteMasterBD(){

        /* PRIMERO ELIMINAMOS EL ARCHIVO ACTUAL */
        /* PRIMERO ELIMINAMOS EL ARCHIVO ACTUAL */
        const filePath = './public/files/Reporte_Master_BD.xlsx';
        console.log('\nESTE ES EL ARCHIVO '+filePath)
        await fs.unlink(filePath, (err) => {
          if (err) {
            console.error('Error al eliminar el archivo:', err);
            return;
          }
          console.log('\nArchivo eliminado exitosamente');
        });
        /* PRIMERO ELIMINAMOS EL ARCHIVO ACTUAL */
        /* PRIMERO ELIMINAMOS EL ARCHIVO ACTUAL */

        /* SEGUNDO CALCULO DEL REPORTE */
        /* SEGUNDO CALCULO DEL REPORTE */
        console.log('\nREPORTE MASTER BD 1');
        var MesesCerrados = await client.query(`
        SELECT 
        ing_fk_responsable
        , ing_fecha
        , del_fk_responsable
        , del_fecha
        , id
        , anio
        , mes
        FROM public.master_bd_mes_cerrado
        where
        del_fk_responsable=0
        `);
    
        var CondicioDelete = '';
        var CondicionSelect = '';
    
        console.log('\n\n CANTIDAD DE MESES CERRADOS '+JSON.stringify(MesesCerrados));
    
        if(MesesCerrados.rows.length>0)
        {
            for(var i=0; i<MesesCerrados.rows.length; i++) 
            {
                if(i==0)
                {
                    CondicioDelete = ` where substring(n_carpeta,2,4)!='`+MesesCerrados.rows[i].anio.substring(2,4)+``+MesesCerrados.rows[i].mes+`' `;
                }
                else
                {
                    CondicioDelete += ` and substring(n_carpeta,2,4)!='`+MesesCerrados.rows[i].anio.substring(2,4)+``+MesesCerrados.rows[i].mes+`' `;
                }
    
                CondicionSelect += ` and substring(d.n_carpeta,2,4)!='`+MesesCerrados.rows[i].anio.substring(2,4)+``+MesesCerrados.rows[i].mes+`' `;
            }
        }
    
        console.log('\n\n CONDICION DELETE '+JSON.stringify(CondicioDelete));
    
        console.log('\n\n CONDICION SELECT '+JSON.stringify(CondicionSelect));
    
        console.log(`\nQUERY DELETE\n delete from public.master_bd `+CondicioDelete+` `);
        await client.query(` delete from public.master_bd `+CondicioDelete+` `);
    
        console.log('\n\n INICIO CALCULO MASTER BD '+moment().format("DD-MM-YYYY HH:mm"));
    
        await client.query(`
        insert into public.master_bd (fecha_creacion, nc_id, n_carpeta, rut, contenedor, id_nave, nombre_nave, eta, m3, monto_din, monto_din_ajuste, monto_carga_usd, monto_carga_clp, monto_carga_clp_ajuste, total_gastos, total_provision, total_a_pagar, monto_pagado, ano, mes, base, aforo, cda, isp, pallets, tvp, otro_transporte, otros, detalle_otro, monto_aju_din, detalle_aju_din, monto_aju_serv, detalle_aju_serv, tc_servicio, precio_base, precio_unitario_x_m3, ejecutivo, mes_eta, din, fk_cliente, din_ingresada_fecha, nombre_cliente, din_pagada_flag, din_pagada_fecha, fk_servicio) 
        SELECT
        DISTINCT
        nc."createdAt" as fecha_creacion
        , nc.id as nc_id
        , coalesce(d.n_carpeta, '') as N_CARPETA
        , coalesce(c.rut, '') as RUT
        , coalesce(d.contenedor, '') as CONTENEDOR
        , coalesce(d.fk_nave, 0) as ID_NAVE
        , coalesce(d.nave_nombre, '[No ingresado]') as NOMBRE_NAVE
        , to_char(d.eta, 'DD-MM-YYYY') as ETA
        , ROUND(nc.carga, 2) as M3
        , ROUND((((coalesce(nc.cif, 0)+coalesce(nc.arancel, 0))*19/100)+coalesce(nc.arancel, 0)+coalesce(nc.impto_port, 0))*coalesce(nc.tc, 0)) as MONTO_DIN
        , ROUND((((coalesce(nc.cif, 0)+coalesce(nc.arancel, 0))*19/100)+coalesce(nc.arancel, 0)+coalesce(nc.impto_port, 0))*coalesce(nc.tc, 0)+coalesce(nc.ajuste_m_1, 0)) as MONTO_DIN_AJUSTE
        , ROUND(coalesce(nc.carga, 0)*coalesce(nc.costo, 0)+coalesce(nc.pb, 0), 2) as MONTO_CARGA_USD
        , ROUND((coalesce(nc.carga, 0)*coalesce(nc.costo, 0)+coalesce(nc.pb, 0))*coalesce(nc.tc2, 0)) as MONTO_CARGA_CLP
        , ROUND((coalesce(nc.carga, 0)*coalesce(nc.costo, 0)+coalesce(nc.pb, 0))*coalesce(nc.tc2, 0)+coalesce(nc.ajuste_m_2, 0)) as MONTO_CARGA_CLP_AJUSTE
        , ROUND((coalesce(nc.carga, 0)*coalesce(nc.costo, 0)+coalesce(nc.pb, 0))*coalesce(nc.tc2, 0)+coalesce(nc.ajuste_m_2, 0) + coalesce(nc.aforo, 0)+coalesce(nc.isp, 0)+coalesce(nc.cda, 0)+coalesce(nc.almacenaje, 0) + coalesce(nc.pallet*nc.pallet_valor_u, 0)+coalesce(nc.ttvp, 0)+coalesce(nc.twsc, 0)+coalesce(nc.otros, 0)) as TOTAL_GASTOS
        , ROUND((((coalesce(nc.cif, 0)+coalesce(nc.arancel, 0))*19/100)+coalesce(nc.arancel, 0)+coalesce(nc.impto_port, 0))*coalesce(nc.tc, 0)+coalesce(nc.ajuste_m_1, 0)+(coalesce(nc.carga, 0)*coalesce(nc.costo, 0)+coalesce(nc.pb, 0))*coalesce(nc.tc2, 0)+coalesce(nc.ajuste_m_2, 0)) as TOTAL_PROVISION
        , ROUND((((coalesce(nc.cif, 0)+coalesce(nc.arancel, 0))*19/100)+coalesce(nc.arancel, 0)+coalesce(nc.impto_port, 0))*coalesce(nc.tc, 0)+coalesce(nc.ajuste_m_1, 0)+(coalesce(nc.carga, 0)*coalesce(nc.costo, 0)+coalesce(nc.pb, 0))*coalesce(nc.tc2, 0)+coalesce(nc.ajuste_m_2, 0) + coalesce(nc.aforo, 0)+coalesce(nc.isp, 0)+coalesce(nc.cda, 0)+coalesce(nc.almacenaje, 0) + coalesce(nc.pallet*nc.pallet_valor_u, 0)+coalesce(nc.ttvp, 0)+coalesce(nc.twsc, 0)+coalesce(nc.otros, 0)) as TOTAL_A_PAGAR
        , ROUND(coalesce((SELECT SUM(dtl.debit_amt) FROM public.wsc_envio_asientos_detalles dtl INNER JOIN public.wsc_envio_asientos_cabeceras cbc ON dtl.fk_cabecera=cbc.id WHERE cbc.carpeta=d.n_carpeta and cbc.estado<>'false' and dtl.desc_text='BANCO SANTANDER'), 0)) as MONTO_PAGADO
        , SUBSTRING(d.n_carpeta, 2, 2) as ANO
        , SUBSTRING(d.n_carpeta, 4, 2) as MES
        , SUBSTRING(d.n_carpeta, 1, 7) as BASE
        , nc.aforo as AFORO
        , nc.cda as CDA
        , nc.isp as ISP
        , coalesce(nc.pallet, 0)*coalesce(nc.pallet_valor_u, 0) as PALLETS
        , coalesce(nc.ttvp, 0) as TVP
        , coalesce(nc.twsc, 0) as OTRO_TRANSPORTE
        , coalesce(nc.otros, 0) as otros
        , coalesce(nc.detalle_otro, '') as detalle_otro
        , coalesce(nc.ajuste_m_1, 0) as MONTO_AJU_DIN
        , coalesce(nc.ajuste_c_1, '') as DETALLE_AJU_DIN
        , coalesce(nc.ajuste_m_2, 0) as MONTO_AJU_SERV
        , coalesce(nc.ajuste_c_2, '') as DETALLE_AJU_SERV
        , ROUND(nc.tc2, 2) as TC_SERVICIO
        , nc.pb as PRECIO_BASE
        , nc.costo as PRECIO_UNITARIO_X_M3
        , coalesce(us.nombre, '') as EJECUTIVO
        , to_char(d.eta, 'MM') as MES_ETA
        , coalesce(nc.din, 0) as din
        , d.fk_cliente
        , coalesce(to_char(nc.din_ingresada_fecha, 'DD-MM-YYYY'), '') as din_ingresada_fecha
        , coalesce(c.codigo, '') as nombre_cliente
        ,
        CASE WHEN nc.din_pagada_flag=true
            THEN 'SI'
            ELSE 'NO'
        END as din_pagada_flag,
        CASE WHEN nc.din_pagada_flag=true
            THEN to_char(nc.din_pagada_fecha, 'DD-MM-YYYY')
            ELSE ''
        END as din_pagada_fecha
        ,
        coalesce((select ct.fk_consolidado
            from tracking t
            inner join consolidado_tracking ct on ct.id = t.fk_consolidado_tracking
            where fk_contenedor = (
            select id from contenedor c
            where codigo = d.contenedor limit 1) and fk_cliente = d.fk_cliente order by ct.fk_consolidado desc limit 1), 0) as fk_servicio
    
            FROM public.despachos d
            LEFT JOIN public.clientes c
                ON d.fk_cliente = c.id
            LEFT JOIN public.usuario us
                ON us.id = c.fk_comercial
            INNER JOIN public.notas_cobros nc
                ON CASE
                WHEN nc.fk_despacho = -1 THEN nc.codigo_unificacion=d.codigo_unificacion
                ELSE nc.fk_despacho = d.id END
        WHERE 
        nc.estado<>false
        `+CondicionSelect+`
        `);
    
        console.log('\n\n FIN CALCULO MASTER BD '+moment().format("DD-MM-YYYY HH:mm"));
    
        var fecha_carga = moment().format("DD-MM-YYYY HH:mm");
    
        console.log(` update public.master_bd set ejecucion_fecha='`+fecha_carga+`', link_descarga='`+process.env.UrlRemoteServer1+`get_master_bd' `);
        await client.query(` update public.master_bd set ejecucion_fecha='`+fecha_carga+`', link_descarga='`+process.env.UrlRemoteServer1+`get_master_bd' `);
        /* SEGUNDO CALCULO DEL REPORTE */
        /* SEGUNDO CALCULO DEL REPORTE */


        /* CREO REPORTE EXCEL DEL REPORTE */
        /* CREO REPORTE EXCEL DEL REPORTE */
        console.log('\n\n INICIO CREAR EXCEL MASTER BD '+moment().format("DD-MM-YYYY HH:mm"));
        var Reporte = await client.query(` 
        SELECT 
        n_carpeta as "N° CARPETA"
        , rut as "RUT"
        , contenedor as "CONTENEDOR"
        , id_nave as "ID NAVE"
        , nombre_nave as "NOMBRE NAVE"
        , eta as "ETA"
        , m3 as "M3"
        , monto_din as "MONTO DIN"
        , monto_din_ajuste as "MONTO DIN AJUSTE"
        , monto_carga_usd as "MONTO CARGA USD"
        , monto_carga_clp as "MONTO CARGA CLP"
        , monto_carga_clp_ajuste as "MONTO CARGA CLP AJUSTE"
        , total_gastos as "TOTAL GASTOS"
        , total_provision as "TOTAL PROVISION"
        , total_a_pagar as "TOTAL A PAGAR"
        , monto_pagado as "MONTO PAGADO"
        , ano as "AÑO"
        , mes as "MES"
        , base as "BASE"
        , aforo as "AFORO"
        , cda as "CDA"
        , isp as "ISP"
        , pallets as "PALLETS"
        , tvp as "TVP"
        , otro_transporte as "OTRO TRANSPORTE"
        , otros as "OTROS"
        , detalle_otro as "DETALLE OTROS"
        , monto_aju_din as "MONTO AJUSTE DIN"
        , detalle_aju_din as "DETALLE AJUSTE DIN"
        , monto_aju_serv as "MONTO AJUSTE SERV"
        , detalle_aju_serv as "DETALLE AJUSTE SERV"
        , tc_servicio as "TC SERVICIO"
        , precio_base as "PRECIO BASE"
        , precio_unitario_x_m3 as "PRECIO UNITARIO XM3"
        , ejecutivo as "EJECUTIVO"
        , mes_eta as "MES ETA"
        , din as "N DIN"
        , fk_cliente as "ID CLIENTE"
        , nombre_cliente as "CODIGO CLIENTE"
        , din_pagada_flag as "DIN PAGADA TESORERIA"
        , din_pagada_fecha as "FECHA PAGO DIN"
        , din_ingresada_fecha as "FECHA INGRESO DIN"
        , fk_servicio as "ID SERVICIO"
        , 'NO' as "DOC FACTURA"
        , 'NO' as "DOC DIN"
        , 'NO' as "DOC F.AGENCIA"
        , 'NO' as "DOC TGR"

        FROM public.master_bd 
        `);
        var xl = require('excel4node');
        const workbook = new xl.Workbook();
        const worksheet = workbook.addWorksheet('Master_Bd');
        const columns = Object.keys(Reporte.rows[0]);

        const numberFormat = workbook.createStyle({
            numberFormat: '#,##0.00', // Usa el formato que necesites (por ejemplo, sin decimales: '#,##0')
          });


        columns.forEach((column, colIndex) => {
            worksheet.cell(1, colIndex + 1).string(column);
        });

        Reporte.rows.forEach((row, rowIndex) => {
            columns.forEach((column, colIndex) => {
                const value = row[column];
                    console.log('\n PROCESANDO COLUMNA '+colIndex);
                    if (
                        colIndex === 7  || colIndex === 8   || colIndex === 9   ||
                        colIndex === 10 || colIndex === 11  || colIndex === 12  ||
                        colIndex === 13 || colIndex === 14  || colIndex === 15  ||
                        colIndex === 19 || colIndex === 20  || colIndex === 21  ||
                        colIndex === 22 || colIndex === 23  || colIndex === 24  ||
                        colIndex === 25 || colIndex === 27  || colIndex === 29  ||
                        colIndex === 31 || colIndex === 32  || colIndex === 33
                    ) 
                    {
                        console.log('\n SI ES COLUMNA NUMERO '+colIndex);
                        // Asegúrate de convertir correctamente a número
                        const numberValue = Number(value.toString().trim()); // Usar trim para eliminar espacios
                        if (!isNaN(numberValue)) 
                        {
                            console.log('\n NO ES NUMERO APLICO FORMATO A COLUMNA '+colIndex+' CONTENIDO '+numberValue);
                            worksheet.cell(rowIndex + 2, colIndex + 1).number(numberValue).style(numberFormat); // Aplica el estilo
                        } 
                        else 
                        {
                            console.log('\n si ES NUMERO COLUMNA '+colIndex+' CONTENIDO '+numberValue);
                            // En caso de no ser un número, guárdalo como cadena para depuración
                            worksheet.cell(rowIndex + 2, colIndex + 1).string(value.toString());
                        }
                    } 
                    else 
                    {
                        worksheet.cell(rowIndex + 2, colIndex + 1).string(value.toString());
                    }
            });
        });
        workbook.write('./public/files/Reporte_Master_BD.xlsx', (err, stats) => {
            if (err) 
            {
                console.error('Error al guardar el archivo Excel:', err);
            } else {
                console.log('\nArchivo Excel guardado exitosamente:', filePath);
            }
        });
        console.log('\n\n FIN CREAR EXCEL MASTER BD '+moment().format("DD-MM-YYYY HH:mm"));
        /* CREO REPORTE EXCEL DEL REPORTE */
        /* CREO REPORTE EXCEL DEL REPORTE */
        
        
        
        console.log('\n\n FIN CREACION ARCHIVO MASTER BD '+moment().format("DD-MM-YYYY HH:mm"));
        /* CREO REPORTE EXCEL DEL REPORTE */
        /* CREO REPORTE EXCEL DEL REPORTE */        

        /* INICIO ENVIANDO REPORTE EXCEL DEL REPORTE */
        /* INICIO ENVIANDO REPORTE EXCEL DEL REPORTE */
        console.log('\n\n INICIO ENVIANDO ARCHIVO MASTER BD '+moment().format("DD-MM-YYYY HH:mm"));
        var Correos = await client.query(`
        SELECT 
        emails
        FROM public.reportes_email
        where
        id=1
        `);
        
        await enviarEmail.mail_reporte_masterbd({
            asunto:'REPORTE MASTER BD '+moment().format("DD-MM-YYYY HH:mm"),
            email:Correos.rows[0]['emails'].split(";"),
        });
        console.log('\n\n FIN ENVIANDO ARCHIVO MASTER BD '+moment().format("DD-MM-YYYY HH:mm"));
        /* INICIO ENVIANDO REPORTE EXCEL DEL REPORTE */
        /* INICIO ENVIANDO REPORTE EXCEL DEL REPORTE */
    }
};

exports.CrearEnviar_ReporteNotasDeCobro = async (req, res) => {

    console.log("\n.::. CrearEnviar_ReporteNotasDeCobro");
    var shc_CrearEnviar_ReporteNotasDeCobro = require('node-schedule');
    shc_CrearEnviar_ReporteNotasDeCobro.scheduleJob('0 9 * * *', () => {
        FUNCT_CrearEnviar_ReporteNotasDeCobro();
    });

    async function FUNCT_CrearEnviar_ReporteNotasDeCobro(){

        /* PRIMERO ELIMINAMOS EL ARCHIVO ACTUAL */
        /* PRIMERO ELIMINAMOS EL ARCHIVO ACTUAL */
        const filePath = './public/files/Reporte_Notas_De_Cobros.xlsx';
        console.log('\nESTE ES EL ARCHIVO '+filePath)
        await fs.unlink(filePath, (err) => {
          if (err) {
            console.error('Error al eliminar el archivo:', err);
            return;
          }
          console.log('\nArchivo eliminado exitosamente');
        });
        /* PRIMERO ELIMINAMOS EL ARCHIVO ACTUAL */
        /* PRIMERO ELIMINAMOS EL ARCHIVO ACTUAL */

        /* CREO REPORTE EXCEL DEL REPORTE */
        /* CREO REPORTE EXCEL DEL REPORTE */
        try {
            console.log('\n\n INICIO CAPTURA REPORTE ARCHIVO NOTAS DE COBRO '+moment().format("DD-MM-YYYY HH:mm"));
            var Reporte = await client.query(` 
            SELECT 
            nc.id as "ID_NOTA",
            fk_despacho as "ID_DESPACHO",
            coalesce(n_carpeta,'') as "N_CARPETA",
            din as "DIN",
            CASE
                WHEN nc.fk_despacho=-1 THEN d.rut
                ELSE cli.rut 
            END as "RUT_CLIENTE",
            CASE
                WHEN nc.fk_despacho=-1 THEN d.codigo_cliente
                ELSE cli.codigo
            END as "COD_CLIENTE",
            CASE
                WHEN nc.fk_despacho=-1 THEN d.fk_cliente
                ELSE cli.id
            END as "ID_CLIENTE",
            u.usuario as "COMERCIAL",
            d.contenedor as "CONTENEDOR",

            coalesce(cif,0) as "CIF",
            coalesce(arancel,0) as "ARANCEL",
            coalesce(impto_port,0) as "IMPUESTO_PORT",
            coalesce(tc,0) as "TC_IMPUESTO",
            coalesce(ajuste_m_1,0) as "AJUSTE_IMPUESTO",
            coalesce(ajuste_c_1,'') as "AJUSTE_IMPUESTO_COM",
            ROUND((((coalesce(nc.cif, 0)+coalesce(nc.arancel, 0))*19/100)+coalesce(nc.arancel, 0)+coalesce(nc.impto_port, 0))*coalesce(nc.tc, 0)+coalesce(nc.ajuste_m_1, 0)) as "TOTAL_IMPUESTO",

            coalesce(m3,0) as "M3",
            coalesce(peso_carga,0) as "PESO_CARGA",
            coalesce(pb,0) as "PRECIO_BASE",
            coalesce(carga,0) as "CARGA",
            coalesce(costo,0) as "USD_M3",
            coalesce(tc2,0) as "TC",
            coalesce(aforo,0) as "AFORO",
            coalesce(isp,0) as "ISP",
            coalesce(cda,0) as "CDA",
            coalesce(almacenaje,0) as "ALMACENAJE",
            coalesce(ajuste_m_2,0) as "AJUSTE_SERVICIO",
            coalesce(ajuste_c_2,'') as "AJUSTE_SERVICIO_COM",
            ROUND((coalesce(nc.carga, 0)*coalesce(nc.costo, 0)+coalesce(nc.pb, 0))*coalesce(nc.tc2, 0)+coalesce(nc.ajuste_m_2, 0)+coalesce(nc.aforo, 0)+coalesce(nc.isp, 0)+coalesce(nc.cda, 0)+coalesce(nc.almacenaje, 0)) as "TOTAL_SERVICIO",


            coalesce(pallet,0) as "N_PALLETS",
            coalesce(pallet_valor_u,0) as "VALOR_U_PALLET",
            coalesce(ttvp,0) as "TRANSP_TVP",
            coalesce(twsc,0) as "TRANSP_WSC",
            coalesce(otros,0) as "OTROS",
            coalesce(ajuste_m_3,0) as "AJUSTE_COMERCIALIZADORA",
            coalesce(ajuste_c_3,'') as "AJUSTE_COMERCIALIZADORA_COM",
            coalesce(pallet*pallet_valor_u+ttvp+twsc+otros,0) as "TOTAL_COMERCIALIZADORA",
            ROUND((
                ROUND
            (
                (
                    coalesce(nc.carga, 0) * coalesce(nc.costo, 0) + coalesce(nc.pb, 0)
                )
                * coalesce(nc.tc2, 0)
                + coalesce(nc.ajuste_m_2, 0)
                + coalesce(nc.aforo, 0)
                + coalesce(nc.isp, 0)
                + coalesce(nc.cda, 0)
                + coalesce(nc.almacenaje, 0) 
                + (
                    (
                        (
                            coalesce(nc.cif, 0)
                            + coalesce(nc.arancel, 0)
                        ) *19/100
                    )
                + coalesce(nc.arancel, 0)
                + coalesce(nc.impto_port, 0))*coalesce(nc.tc, 0)
                + coalesce(nc.ajuste_m_1, 0) 
                + coalesce(nc.pallet, 0)
                * coalesce(nc.pallet_valor_u, 0)
                + coalesce(nc.ttvp, 0)
                + coalesce(nc.twsc, 0)+coalesce(nc.otros, 0)
            ) + coalesce(afecto*carga*0.19*tc2,0) 
            )) as "TOTAL",
            coalesce(d.nave_nombre, 'NO ESPECIFICADO') as "NOMBRE_NAVE",
            coalesce((select ct.fk_consolidado
                from tracking t
                inner join consolidado_tracking ct on ct.id = t.fk_consolidado_tracking
                where fk_contenedor = (
                select id from contenedor c
                where codigo = d.contenedor limit 1) and fk_cliente = d.fk_cliente order by ct.fk_consolidado desc limit 1), 0) as "ID_SERVICIO"
            , 'INDEFINIDO' as "DOC_FACTURA"
            , 'INDEFINIDO' as "DOC_DIN"
            , 'INDEFINIDO' as "DOC_F_AGENCIA"
            , 'INDEFINIDO' as "DOC_TGR"

            FROM public.notas_cobros nc
            INNER JOIN public.despachos d
                ON CASE
                        WHEN nc.fk_despacho = -1 THEN nc.codigo_unificacion=d.codigo_unificacion
                        ELSE nc.fk_despacho = d.id END
            LEFT JOIN public.clientes cli
                ON cli.id=d.fk_cliente
            INNER JOIN public.usuario u
                ON u.id=cli.fk_comercial
            WHERE nc.estado=true
            `);
            console.log('\n\n FIN CAPTURA REPORTE ARCHIVO NOTAS DE COBRO '+moment().format("DD-MM-YYYY HH:mm"));
                    
            console.log('\n\n INICIO CREAR EXCEL NOTAS DE COBRO '+moment().format("DD-MM-YYYY HH:mm"));
            var xl = require('excel4node');
            const workbook = new xl.Workbook();
            const worksheet = workbook.addWorksheet('Reporte');
            const columns = Object.keys(Reporte.rows[0]);

            const numberFormat = workbook.createStyle({
                numberFormat: '#,##0.00', // Usa el formato que necesites (por ejemplo, sin decimales: '#,##0')
              });

            columns.forEach((column, colIndex) => {
                worksheet.cell(1, colIndex + 1).string(column);
            });

            Reporte.rows.forEach((row, rowIndex) => {
                columns.forEach((column, colIndex) => {
                    const value = row[column];
                    console.log('\n PROCESANDO COLUMNA '+colIndex);
                    if (
                        colIndex === 3 ||
                        colIndex === 6 || colIndex === 9 || colIndex === 10 ||
                        colIndex === 11 || colIndex === 12 || colIndex === 13 ||
                        colIndex === 14 || colIndex === 15 || colIndex === 16 ||
                        colIndex === 17 || colIndex === 18 || colIndex === 19 ||
                        colIndex === 20 || colIndex === 21 || colIndex === 22 ||
                        colIndex === 23 || colIndex === 24 || colIndex === 25 ||
                        colIndex === 26 || colIndex === 28 || colIndex === 29 ||
                        colIndex === 30 || colIndex === 31 || colIndex === 32 ||
                        colIndex === 33 || colIndex === 34 || colIndex === 35 ||
                        colIndex === 36 || colIndex === 37
                    ) 
                    {
                        console.log('\n SI ES COLUMNA NUMERO '+colIndex);
                        // Asegúrate de convertir correctamente a número
                        const numberValue = Number(value.toString().trim()); // Usar trim para eliminar espacios
                        if (!isNaN(numberValue)) 
                        {
                            console.log('\n NO ES NUMERO APLICO FORMATO A COLUMNA '+colIndex+' CONTENIDO '+numberValue);
                            worksheet.cell(rowIndex + 2, colIndex + 1).number(numberValue).style(numberFormat); // Aplica el estilo
                        } 
                        else 
                        {
                            console.log('\n si ES NUMERO COLUMNA '+colIndex+' CONTENIDO '+numberValue);
                            // En caso de no ser un número, guárdalo como cadena para depuración
                            worksheet.cell(rowIndex + 2, colIndex + 1).string(value.toString());
                        }
                    } 
                    else 
                    {
                        worksheet.cell(rowIndex + 2, colIndex + 1).string(value.toString());
                    }
                });
            });
            workbook.write('./public/files/Reporte_Notas_De_Cobros.xlsx', (err, stats) => {
                if (err) 
                {
                    console.error('Error al guardar el archivo Excel:', err);
                } else {
                    console.log('\nArchivo Excel guardado exitosamente:', filePath);
                }
            });
            console.log('\n\n FIN CREAR EXCEL NOTAS DE COBRO '+moment().format("DD-MM-YYYY HH:mm"));
            /* CREO REPORTE EXCEL DEL REPORTE */
            /* CREO REPORTE EXCEL DEL REPORTE */

        } catch (err) {
            console.error('Error al generar el reporte:', err);
        } 

        /* ENVIANDO REPORTE POR CORREO */
        /* ENVIANDO REPORTE POR CORREO */
        console.log('\n\n INICIO ENVIANDO ARCHIVO NOTAS DE COBRO '+moment().format("DD-MM-YYYY HH:mm"));
        var Correos = await client.query(`
        SELECT 
        emails
        FROM public.reportes_email
        where
        id=2
        `);
        await enviarEmail.mail_reporte_notasdecobro({
            asunto:'REPORTE NOTAS DE COBRO '+moment().format("DD-MM-YYYY HH:mm"),
            email:Correos.rows[0]['emails'].split(";"),
        });
        console.log('\n\n FIN ENVIANDO ARCHIVO NOTAS DE COBRO '+moment().format("DD-MM-YYYY HH:mm"));
        /* ENVIANDO REPORTE POR CORREO */
        /* ENVIANDO REPORTE POR CORREO */
    }
};

exports.CrearEnviar_ReporteClientes = async (req, res) => {

    console.log("\n.::.");
    console.log("\n.::.");
    var shc_CrearEnviar_ReporteClientes = require('node-schedule');
    shc_CrearEnviar_ReporteClientes.scheduleJob('30 9 * * *', () => {
        FUNCT_CrearEnviar_ReporteClientes();
    });

    async function FUNCT_CrearEnviar_ReporteClientes(){

        /* PRIMERO ELIMINAMOS EL ARCHIVO ACTUAL */
        /* PRIMERO ELIMINAMOS EL ARCHIVO ACTUAL */
        const filePath = './public/files/Reporte_Clientes.xlsx';
        console.log('\nESTE ES EL ARCHIVO '+filePath)
        await fs.unlink(filePath, (err) => {
          if (err) {
            console.error('Error al eliminar el archivo:', err);
            return;
          }
          console.log('\nArchivo eliminado exitosamente');
        });
        /* PRIMERO ELIMINAMOS EL ARCHIVO ACTUAL */
        /* PRIMERO ELIMINAMOS EL ARCHIVO ACTUAL */

        /* CREO REPORTE EXCEL DEL REPORTE */
        /* CREO REPORTE EXCEL DEL REPORTE */
        try {
            console.log('\n\n INICIO CAPTURA REPORTE ARCHIVO CLIENTES '+moment().format("DD-MM-YYYY HH:mm"));
            var Reporte = await client.query(` 
            SELECT 
            concat(coalesce(cli.id::text, ''), ' ', coalesce(cli.codigo, '')) as "ID DWI",
            coalesce(cli.id, 0) as "ID WSC",
            upper(coalesce(cli.codigo, '')) as "NOMBRE CORTO",
            upper(coalesce(cli.rut, '')) as "RUT",
            upper(coalesce(cli."razonSocial", '')) as "RAZON SOCIAL",
            upper(coalesce(cli."codigoSii", '')) as "CODIGO SII",
            upper(coalesce(UPPER(cli.giro), '')) as "GIRO",
            upper(coalesce(cli.telefono1, '')) as "TELEFONO 1",
            upper(coalesce(cli."dteEmail", '')) as "DTE EMAIL",
            upper(concat(coalesce(dir_1.direccion, ''), ' ', coalesce(dir_1.numero, ''))) as "DIRECCION",
            upper(coalesce(comu_1.nombre, '')) as "COMUNA",
            upper(coalesce(comu_1.codigo_maximize, '')) as "COMUNA MAXIMISE",
            upper(coalesce(reg_1.nombre, '')) as "REGION",
            upper(coalesce(rep_1.nombre, '')) as "REP NOMBRES",
            '' as "SEGUNDO NOMBRE",
            upper(coalesce(rep_1.apellido, '')) as "REP APELLIDOS",
            '' as "APELLIDO MATERNO",
            upper(coalesce(rep_1.rut, '')) as "REP RUT",
            upper(coalesce(rep_1.email, '')) as "REP EMAIL",
            upper(coalesce(cli.giro, '')) as "GIRO",
            '' as "CONTACTO NOMBRE",
            '' as "CONTACTO EMAIL",
            '' as "CONTACTO TELEFONO",
            '' as "CONTACTO DIRECCION",
            'WS Cargo' as "TIPO CLIENTE",
            upper(coalesce(comer.nombre, '')) as "COMERCIAL",
            TO_CHAR(cli."fechaCreacion"::date, 'DD/MM/YYYY') as "FECHA CREACION",
            CASE WHEN 
                coalesce(rep_file_1.cedula_1, '') <> '' AND 
                coalesce(rep_file_1.cedula_1_type, '') <> '' AND 
                coalesce(rep_file_1.cedula_1_ext, '') <> ''
            THEN 'VERDADERO' ELSE 'VERDADERO' END as "REP CI",
            CASE WHEN 
                coalesce(rep_file_1.podersimple_1, '') <> '' AND 
                coalesce(rep_file_1.podersimple_1_type, '') <> '' AND 
                coalesce(rep_file_1.podersimple_1_ext, '') <> ''
            THEN 'VERDADERO' ELSE 'VERDADERO' END as "REP PODER",
            '' as "COMUNA MAXIMISE",
            '' as "REGION MAXIMISE",
            CASE WHEN cli.estado_consolidado = 'SI' THEN 'CLIENTE' ELSE 'OPORTUNIDAD' END as "ESTADO COMERCIAL",
            coalesce((SELECT sum(t.volumen_recepcionado) FROM public.tracking t WHERE t.fk_cliente = cli.id AND t.estado >= 0), 0) as "VOLUMEN TOTAL",
            coalesce((SELECT to_char(t.fecha_recepcion, 'DD-MM-YYYY') FROM public.tracking t WHERE t.fk_cliente = cli.id ORDER BY t.id asc LIMIT 1), '') as "FECHA RECEPCION",
            coalesce(ori.nombre,'INDEFINIDO') as ORIGEN
            FROM 
                public.clientes as cli
            left join public.gc_contactos_tipos as ori on coalesce(cli.fk_origen,0)=ori.id
            LEFT JOIN public.clientes_direcciones as dir_1 ON dir_1.id = (
                SELECT temp1.id FROM public.clientes_direcciones as temp1 
                WHERE temp1.fk_cliente = cli.id AND temp1.fk_tipo = 1 
                ORDER BY temp1.id DESC LIMIT 1 
            )
            LEFT JOIN public.comunas as comu_1 ON dir_1.fk_comuna = comu_1.id
            LEFT JOIN public.region as reg_1 ON dir_1.fk_region = reg_1.id
            LEFT JOIN public.usuario as comer ON cli.fk_comercial = comer.id
            LEFT JOIN public.clientes_contactos as rep_1 ON rep_1.id = (
                SELECT temp1.id FROM public.clientes_contactos as temp1 
                WHERE cli.id = temp1.fk_cliente AND temp1.fk_tipo = 4 AND temp1.estado IS TRUE 
                ORDER BY temp1.id DESC LIMIT 1 
            )
            LEFT JOIN public.clientes_contactos_archivos as rep_file_1 ON rep_file_1.id = (
                SELECT temp1.id FROM public.clientes_contactos_archivos as temp1 
                WHERE rep_1.id = temp1.fk_contacto 
                ORDER BY temp1.id DESC LIMIT 1 
            )
            `);
            console.log('\n\n FIN CAPTURA REPORTE ARCHIVO CLIENTES '+moment().format("DD-MM-YYYY HH:mm"));
                    
            console.log('\n\n INICIO CREAR EXCEL CLIENTES '+moment().format("DD-MM-YYYY HH:mm"));
            var xl = require('excel4node');
            const workbook = new xl.Workbook();
            const worksheet = workbook.addWorksheet('Hoja1');
            const columns = Object.keys(Reporte.rows[0]);
            columns.forEach((column, colIndex) => {
                worksheet.cell(1, colIndex + 1).string(column);
            });
            Reporte.rows.forEach((row, rowIndex) => {
                columns.forEach((column, colIndex) => {
                    const value = row[column];
                    if (typeof value === 'number') {
                    worksheet.cell(rowIndex + 2, colIndex + 1).number(value);
                    } else {
                    worksheet.cell(rowIndex + 2, colIndex + 1).string(value.toString());
                    }
                });
            });
            workbook.write('./public/files/Reporte_Clientes.xlsx', (err, stats) => {
                if (err) 
                {
                    console.error('Error al guardar el archivo Excel:', err);
                } else {
                    console.log('\nArchivo Excel guardado exitosamente:', filePath);
                }
            });
            console.log('\n\n FIN CREAR EXCEL CLIENTES '+moment().format("DD-MM-YYYY HH:mm"));
            /* CREO REPORTE EXCEL DEL REPORTE */
            /* CREO REPORTE EXCEL DEL REPORTE */

        } catch (err) {
            console.error('Error al generar el reporte:', err);
        } 

        /* ENVIANDO REPORTE POR CORREO */
        /* ENVIANDO REPORTE POR CORREO */
        console.log('\n\n INICIO ENVIANDO ARCHIVO REPORTE CLIENTES '+moment().format("DD-MM-YYYY HH:mm"));
        var Correos = await client.query(`
        SELECT 
        emails
        FROM public.reportes_email
        where
        id=4
        `);
        await enviarEmail.mail_reporte_clientes({
            asunto:'REPORTE CLIENTES '+moment().format("DD-MM-YYYY HH:mm"),
            email:Correos.rows[0]['emails'].split(";"),
        });
        console.log('\n\n FIN ENVIANDO ARCHIVO REPORTE CLIENTES '+moment().format("DD-MM-YYYY HH:mm"));
        /* ENVIANDO REPORTE POR CORREO */
        /* ENVIANDO REPORTE POR CORREO */
    }
};