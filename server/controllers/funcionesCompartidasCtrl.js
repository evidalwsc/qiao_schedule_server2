const client = require('../config/db.client');
const axios = require("axios");
const { json } = require('body-parser');
const e = require('express');
const clientExp = require('../config/db.client.experienciadigital');
const moment=require('moment');


exports.GuardarServicioEnExperienciaDigital=async(servicio)=>{
    try{
        const{
            pesoEstimado,
            volumenEstimado,
            valorUnitarioUsd,
            tarifaUsd,
            valorBaseUsd,
            unidadesACobrar,
            cmbPeso,
            bultos,
            fk_comercial,
            fk_cliente,
            fk_zonaOrigen,
            fk_zonaDestino,
            fk_consolidado,
            fk_servicio,
            trackings
        }=servicio;

        let idUser=null;

        let sqlUser= await clientExp.query(`SELECT fk_usuario FROM public.registro WHERE fk_cliente=`+fk_cliente);
        
        if(sqlUser && sqlUser.rows && sqlUser.rows.length>0)
        {
            idUser=sqlUser.rows[0].fk_usuario;
        }

        if(fk_servicio==null)
        {
            let sqlService = {
                text:`INSERT INTO public.servicio(
                    estado, peso, volumen, descripcion, tarifa, total, "valorBaseUsd", "unidadesACobrar", "cmbPeso", cantidad_bultos, fk_comercial, fk_cliente, fk_usuario, fk_origen, fk_destino, fecha_creacion, exported,fk_consolidado)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18) RETURNING*`,
                values:[true,pesoEstimado,volumenEstimado,'IMPORT WSC',valorUnitarioUsd,tarifaUsd,valorBaseUsd,unidadesACobrar,cmbPeso,bultos,fk_comercial,fk_cliente,idUser,fk_zonaOrigen,fk_zonaDestino,moment().format('YYYYMMDD HHmmss'),true, fk_consolidado]
            }
            let insertService = await clientExp.query(sqlService);

            if(insertService && insertService.rows && insertService.rows.length>0)
            {
                if(trackings  && trackings.length>0)
                {
                    for(let j=0;j<trackings.length;j++)
                    {
                        let aprob1=false;let aprob2=false;
                        if(trackings[j].packing_list1)
                        {
                            aprob1=true;
                        }

                        if(trackings[j].invoice1)
                        {
                            aprob2=true;
                        }
                        
                        let sqlServiceP={
                            text:`INSERT INTO public.servicio_proveedor(
                                nombre, nombre_chino, cantidad_bultos, peso, volumen, estado, exported, fk_tracking,fk_servicio,packing_list1_aprob,packing_list2_aprob)
                                VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)`,
                            values:[trackings[j].fk_proveedor_nombre,trackings[j].fk_proveedor_nombre_chino,trackings[j].cantidad_bultos,trackings[j].peso,trackings[j].volumen,true,true,trackings[j].id,insertService.rows[0].id,aprob1,aprob2]
                        }

                        let insertServiceP = await clientExp.query(sqlServiceP);
                    }
                }
                return insertService.rows[0];
            }
        }
        else
        {
            let sqlService={
                text:`UPDATE public.servicio SET estado=$1,peso=$2,volumen=$3, tarifa=$4, total=$5, "valorBaseUsd"=$6, "unidadesACobrar"=$7, "cmbPeso"=$8, cantidad_bultos=$9, fk_comercial=$10, fk_cliente=$11, fk_usuario=$12, fk_origen=$13, fk_destino=$14, exported=$15,fk_consolidado=$16 WHERE id=$17 RETURNING*`,
                values:[true,pesoEstimado,volumenEstimado,valorUnitarioUsd,tarifaUsd,valorBaseUsd,unidadesACobrar,cmbPeso,bultos,fk_comercial,fk_cliente,idUser,fk_zonaOrigen,fk_zonaDestino,true, fk_consolidado,fk_servicio]
            }

            let updateService = await clientExp.query(sqlService);

            if(updateService && updateService.rows && updateService.rows.length>0)
            {
                let queryIn='';
                if(trackings && trackings.length>0)
                {
                    for(let i=0;i<trackings.length;i++)
                    {
                        let aprob1=false;let aprob2=false;
                        
                        if(trackings[i].packing_list1)
                        {
                            aprob1=true;
                        }

                        if(trackings[i].invoice1)
                        {
                            aprob2=true;
                        }
                        
                        let exists = await clientExp.query(`SELECT *FROM public.servicio_proveedor WHERE fk_tracking=`+trackings[i].id+` and fk_servicio=`+fk_servicio);
                        
                        if(exists && exists.rows && exists.rows.length>0)
                        {
                            let sqlUpdateProv={
                                text:`UPDATE public.servicio_proveedor
                                SET nombre=$1, nombre_chino=$2, cantidad_bultos=$3, peso=$4, volumen=$5,  estado=$6, exported=$7,packing_list1_aprob=$8,packing_list2_aprob=$9
                                WHERE fk_tracking=$10 and fk_servicio=$11`,
                                values:[trackings[i].fk_proveedor_nombre,trackings[i].fk_proveedor_nombre_chino,trackings[i].cantidad_bultos,trackings[i].peso,trackings[i].volumen,true,true,aprob1,aprob2,trackings[i].id,fk_servicio]
                            }
                            let updateProv=await clientExp.query(sqlUpdateProv);
                        }
                        else
                        {
                            let sqlServiceP={
                                text:`INSERT INTO public.servicio_proveedor(
                                    nombre, nombre_chino, cantidad_bultos, peso, volumen, estado, exported,fk_tracking,fk_servicio,packing_list1_aprob,packing_list2_aprob)
                                    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)`,
                                values:[trackings[i].fk_proveedor_nombre,trackings[i].fk_proveedor_nombre_chino,trackings[i].cantidad_bultos,trackings[i].peso,trackings[i].volumen,true,true,trackings[i].id,fk_servicio,aprob1,aprob2]
                            }
                            let insertServiceP = await clientExp.query(sqlServiceP);
                        }

                        if(i!==trackings.length-1)
                        {
		        			queryIn+=trackings[i].id+',';
		        		}
                        else
                        {
		        			queryIn+=trackings[i].id;
		        		}
                    }

                    if(queryIn!='')
                    {
                        await client.query(`UPDATE public.servicio_proveedor SET estado=false WHERE fk_servicio=`+fk_servicio+` and fk_tracking not in(`+queryIn+`)`);
                    }
                }

                let bultosx=await clientExp.query(`
                SELECT SUM(cantidad_bultos) as bultos
                FROM public.servicio_proveedor 
                where 
                fk_servicio=`+fk_servicio+` and estado=true;
                `);
                
                if(bultosx && bultosx.rows && bultosx.rows.length>0)
                {
                    await clientExp.query(`UPDATE public.servicio SET cantidad_bultos=`+parseInt(bultosx.rows[0].bultos)+` WHERE id=`+fk_servicio)
                }

                return updateService.rows[0];
            }
        }
    }
    catch (error) 
    {
        console.log("\n\nError crear servicio en experiencia digital "+error);
        return error;
    }
}

exports.FirmaDigital_EnviarEnrolamiento = async (fk_cliente) => {

    var InfoCliente = await client.query(`
    SELECT
    cli.id as id_ws
    , coalesce(cli.codigo,'') as nombre_corto
    , coalesce(cli.rut,'') as rut
    , coalesce(cli."razonSocial",'') as cliente
    , cli.telefono1
    , cli."dteEmail"
    , coalesce(rep.nombre,'') as rep_nombre
    , coalesce(rep.apellido,'') as rep_apellidos
    , coalesce(rep.rut,'') as rep_rut
    , coalesce(replace(rep.email,' ',''),'') as rep_email
    , coalesce(rep.firma_digital_estado,'') as rep_firma_estado
    , coalesce(rep.firma_digital_id,'') as rep_firma_id
    , cli.firma_digital_estado
    , cli.firma_digital_grupo
    , cli.firma_digital_mandato
    , cli.firma_digital_vigencia

    , concat(dir.direccion,' Nº ',dir.numero,', ',com.nombre,', ',reg.nombre) as direccion
    FROM
    public.clientes as cli

    inner join public.clientes_contactos as rep on rep.id=(SELECT
    temp1.id
    from public.clientes_contactos as temp1
    where
    temp1.fk_cliente=cli.id
    and temp1.fk_tipo=4
    and temp1.estado is true order by temp1.id desc limit 1)


    inner join public.clientes_direcciones as dir on dir.id=(SELECT
    temp1.id
    from public.clientes_direcciones as temp1
    where temp1.fk_tipo=1 and temp1.fk_cliente=cli.id order by temp1.id desc limit 1)
    inner join public.comunas as com on dir.fk_comuna=com.id
    inner join public.region as reg on dir.fk_region=reg.id

    where
    coalesce(cli.firma_digital_estado,'')='PENDIENTE ENVIO'

    group by
    cli.id
    , coalesce(cli.codigo,'')
    , coalesce(cli.rut,'')
    , coalesce(cli."razonSocial",'')
    , cli.telefono1
    , cli."dteEmail"
    , coalesce(rep.nombre,'')
    , coalesce(rep.apellido,'')
    , coalesce(rep.rut,'')
    , coalesce(replace(rep.email,' ',''),'')
    , coalesce(rep.firma_digital_estado,'')
    , coalesce(rep.firma_digital_id,'')
    , concat(dir.direccion,' Nº ',dir.numero,', ',com.nombre,', ',reg.nombre)
    , cli.firma_digital_estado
    , cli.firma_digital_grupo
    , cli.firma_digital_mandato
    , cli.firma_digital_vigencia

    order by
    cli.id
    asc
    limit 1
    `);

    if(InfoCliente && InfoCliente.rows && InfoCliente.rows.length>0)
    {
        const fs = require("fs");
        const pdf = require('html-pdf');

        let rep_nombre = InfoCliente.rows[0].rep_nombre.toUpperCase()+' '+InfoCliente.rows[0].rep_apellidos.toUpperCase().trim().trim().trim();
        let rep_rut = InfoCliente.rows[0].rep_rut.toUpperCase().trim().trim().trim();
        let rep_email = InfoCliente.rows[0].rep_email.toLowerCase().trim().trim().trim();
        let rep_firma_estado = InfoCliente.rows[0].rep_firma_estado.toLowerCase().trim().trim().trim();
        let cliente = InfoCliente.rows[0].cliente.toUpperCase().trim().trim().trim();
        let direccion = InfoCliente.rows[0].direccion.toUpperCase().trim().trim().trim();
        let cliente_nombre_corto = InfoCliente.rows[0].nombre_corto.toUpperCase().trim().trim().trim();

        let cli_id = InfoCliente.rows[0].id_ws;

        if( rep_nombre.length>5 && rep_rut.length>=8 && rep_email.length>5 && cliente.length>=1 && direccion.length>=5)
        {
            console.log(".::.");
            console.log(".::.");
            console.log(".::.");
            console.log("Genrando PDF para cliente "+InfoCliente.rows[0].id_ws);

            var fecha =  new Date();
            var moment = require('moment'); moment.locale('es');
            var mandato = 'files/mandatos/'+InfoCliente.rows[0].id_ws+'_'+moment(fecha).format('YYYYMMDDHHmmss')+'.pdf';
            fecha = moment(fecha).format('LL').toUpperCase();

            const content = `
            <!DOCTYPE html>
            <html>
            <head>
            <title>MANDATO</title>
            </head>
            <style>
                .page{
                    size: 21cm 29.7cm;
                    margin: 10mm 10mm 10mm 10mm;
                }
                @media print {
                .break-page {
                    page-break-after: always;
                }
            }
            </style>
            <body class="page">

                <p style="text-align:center">
                    <span style="font-size:11pt">
                        <span style="font-family:Calibri,sans-serif">
                            <u>
                                <span style="font-size:12.0pt">
                                    <span style="font-family:&quot;Garamond&quot;,serif">
                                        MANDATO PARA AGENTE DE ADUANAS
                                    </span>
                                </span>
                            </u>
                        </span>
                    </span>
                </p>

                <p style="text-align:justify">&nbsp;</p>

                <p style="text-align:justify">
                    <span style="font-size:11pt">
                        <span style="font-family:Calibri,sans-serif">
                            <span style="font-size:12.0pt">
                                <span style="font-family:&quot;Garamond&quot;,serif">
                                    Por medios digitales, con fecha <strong>`+fecha+`</strong>, en este acto suscribe el presente documento, don <strong>`+rep_nombre+`</strong>, cédula de identidad Nº <strong>`+rep_rut+`</strong>, correo electrónico <strong>`+rep_email+`</strong>, en representación legal de <strong>`+cliente+`</strong>, según se acreditará, ambos con domicilio en <strong>`+direccion+`</strong>, (en adelante el &ldquo;<u>MANDANTE</u>&rdquo;), quien expone:
                                </span>
                            </span>
                        </span>
                    </span>
                </p>

                <p style="text-align:justify">
                    <span style="font-size:11pt">
                        <span style="font-family:Calibri,sans-serif">
                            <strong>
                                <u>
                                    <span style="font-size:12.0pt">
                                        <span style="font-family:&quot;Garamond&quot;,serif">PRIMERO</span>
                                    </span>
                                </u>
                            </strong>
                            <span style="font-size:12.0pt">
                                <span style="font-family:&quot;Garamond&quot;,serif">
                                    : <strong>CONSTITUCIÓN DE MANDATO.</strong>
                                </span>
                            </span>
                        </span>
                    </span>
                </p>

                <p style="text-align:justify">
                    <span style="font-size:11pt">
                        <span style="font-family:Calibri,sans-serif">
                            <span style="font-size:12.0pt">
                                <span style="font-family:&quot;Garamond&quot;,serif">
                                    Que, por medio de este acto, el MANDANTE viene en conferir un mandato (en adelante el &ldquo;<u>MANDATO</u>&rdquo;) en los t&eacute;rminos establecidos en el art&iacute;culo 197 del Decreto con Fuerza de Ley N&deg;30 que aprueba el texto refundido, coordinado y sistematizado del decreto con fuerza de ley de hacienda N&ordm; 213, de 1953, sobre ordenanza de aduanas, pero tan amplio y bastante como en derecho se requiere en favor de don <strong>Hernan Adolfo Soto Ulloa</strong>, agente de aduanas, c&eacute;dula de identidad n&uacute;mero 9.264.772-2 (en adelante el &ldquo;<u>MANDATARIO</u>&rdquo;), para que en su nombre y representaci&oacute;n se encargue del despacho de sus mercanc&iacute;as en el ingreso al territorio de la Rep&uacute;blica de Chile. 
                                </span>
                            </span>
                        </span>
                    </span>
                </p>

                <p style="text-align:justify">
                    <span style="font-size:11pt">
                        <span style="font-family:Calibri,sans-serif">
                            <strong>
                                <u>
                                    <span style="font-size:12.0pt">
                                        <span style="font-family:&quot;Garamond&quot;,serif">
                                            SEGUNDO
                                        </span>
                                    </span>
                                </u>
                            </strong>
                            <span style="font-size:12.0pt">
                                <span style="font-family:&quot;Garamond&quot;,serif">
                                    : <strong>FACULTADES DEL MANDATARIO.</strong>
                                </span>
                            </span>
                        </span>
                    </span>
                </p>

                <p style="text-align:justify">
                    <span style="font-size:11pt">
                        <span style="font-family:Calibri,sans-serif">
                            <span style="font-size:12.0pt">
                                <span style="font-family:&quot;Garamond&quot;,serif">
                                    En ejercicio de su encargo el MANDATARIO estar&aacute; premunido de las siguientes facultades:
                                </span>
                            </span>
                        </span>
                    </span>
                </p>

                <ol style="list-style-type:lower-alpha">
                    <li style="text-align:justify">
                        <span style="font-size:11pt">
                            <span style="font-family:Calibri,sans-serif">
                                <span style="font-size:12.0pt"><span style="font-family:&quot;Garamond&quot;,serif">
                                    Retirar las mercanc&iacute;as de la potestad aduanera.
                                </span>
                            </span>
                        </span>
                    </span>
                </li>
                    <li style="text-align:justify">
                        <span style="font-size:11pt">
                            <span style="font-family:Calibri,sans-serif">
                                <span style="font-size:12.0pt"><span style="font-family:&quot;Garamond&quot;,serif">
                                    Formular peticiones y reclamaciones ante la Autoridad Aduanera.
                                </span>
                            </span>
                        </span>
                    </span>
                </li>
                    <li style="text-align:justify">
                        <span style="font-size:11pt">
                            <span style="font-family:Calibri,sans-serif">
                                <span style="font-size:12.0pt">
                                    <span style="font-family:&quot;Garamond&quot;,serif">
                                        Solicitar y percibir por v&iacute;a administrativa devoluciones de dineros o cualquier otra que sea consecuencia del despacho.
                                    </span>
                                </span>
                            </span>
                        </span>
                    </li>
                    <li style="text-align:justify">
                        <span style="font-size:11pt">
                            <span style="font-family:Calibri,sans-serif">
                                <span style="font-size:12.0pt">
                                    <span style="font-family:&quot;Garamond&quot;,serif">
                                        En general, realizar todos los actos o tr&aacute;mites relacionados directamente con el despacho encargado.
                                    </span>
                                </span>
                            </span>
                        </span>
                    </li>
                </ol>

                <p style="text-align:justify">
                    <span style="font-size:11pt">
                        <span style="font-family:Calibri,sans-serif">
                            <strong>
                                <u>
                                    <span style="font-size:12.0pt">
                                        <span style="font-family:&quot;Garamond&quot;,serif">
                                            TERCERO
                                        </span>
                                    </span>
                                </u>
                            </strong>
                            <span style="font-size:12.0pt">
                                <span style="font-family:&quot;Garamond&quot;,serif">
                                    : <strong>VIGENCIA DEL MANDATO.</strong>
                                </span>
                            </span>
                        </span>
                    </span>
                </p>

                <p style="text-align:justify">
                    <span style="font-size:11pt">
                        <span style="font-family:Calibri,sans-serif">
                            <span style="font-size:12.0pt">
                                <span style="font-family:&quot;Garamond&quot;,serif">
                                    El presente mandato tendr&aacute; una duraci&oacute;n de 1 a&ntilde;o contado desde la fecha en que se suscribe este instrumento y se entiende conferido para todos los despachos que ocurran en dicho per&iacute;odo.
                                </span>
                            </span>
                        </span>
                    </span>
                </p>

                <p style="text-align:justify">
                    <span style="font-size:11pt">
                        <span style="font-family:Calibri,sans-serif">
                            <strong>
                                <u>
                                    <span style="font-size:12.0pt">
                                        <span style="font-family:&quot;Garamond&quot;,serif">
                                            CUARTO
                                        </span>
                                    </span>
                                </u>
                            </strong>
                            <span style="font-size:12.0pt">
                                <span style="font-family:&quot;Garamond&quot;,serif">
                                    : <strong>VALIDEZ LEGAL.</strong>
                                </span>
                            </span>
                        </span>
                    </span>
                </p>

                <p style="text-align:justify">
                    <span style="font-size:11pt">
                        <span style="font-family:Calibri,sans-serif">
                            <span style="font-size:12.0pt">
                                <span style="font-family:&quot;Garamond&quot;,serif">
                                    Se otorga el presente mandato de conformidad a la Resoluci&oacute;n Exenta N&deg; 2299 de fecha 30 de septiembre de 2021, modificada por Resoluci&oacute;n Exenta N&deg; 2416 de fecha 13 de octubre de 2021, ambas del Sr. Director Nacional de Aduanas.
                                </span>
                            </span>
                        </span>
                    </span>
                </p>

                <p style="text-align:justify">
                    <span style="font-size:11pt">
                        <span style="font-family:Calibri,sans-serif">
                            <strong>
                                <u>
                                    <span style="font-size:12.0pt">
                                        <span style="font-family:&quot;Garamond&quot;,serif">
                                            QUINTO
                                        </span>
                                    </span>
                                </u>
                            </strong>
                            <span style="font-size:12.0pt">
                                <span style="font-family:&quot;Garamond&quot;,serif">
                                    : <strong>FIRMA.</strong>
                                </span>
                            </span>
                        </span>
                    </span>
                </p>

                <p style="text-align:justify">
                    <span style="font-size:11pt">
                        <span style="font-family:Calibri,sans-serif">
                            <span style="font-size:12.0pt">
                                <span style="font-family:&quot;Garamond&quot;,serif">
                                    Se suscribe este documento con firma electr&oacute;nica avanzada por el MANDANTE, la cual es provista por la entidad acreditadora BPO-Advisors (IDok) Acreditada seg&uacute;n&nbsp;R.A.E. N&ordm; 3696, de fecha 6 de&nbsp;noviembre de 2017, de la Subsecretar&iacute;a de Econom&iacute;a y Empresas de Menor Tama&ntilde;o.
                                </span>
                            </span>
                        </span>
                    </span>
                </p>

                <p style="text-align:justify">
                    <span style="font-size:11pt">
                        <span style="font-family:Calibri,sans-serif">
                            <span style="font-size:12.0pt">
                                <span style="font-family:&quot;Garamond&quot;,serif">
                                    La validez de la firma que se inserta a este documento pueden verificarse en: 
                                </span>
                            </span>
                            <a href="https://firmaya.idok.cl/doc_validator" style="color:#0563c1; text-decoration:underline">
                                <span style="font-size:12.0pt">
                                    <span style="font-family:&quot;Garamond&quot;,serif">
                                        https://firmaya.idok.cl/doc_validator
                                    </span>
                                </span>
                            </a>
                        </span>
                    </span>
                </p>

            </body>
            </html>
            `;

            var options = { "height": "380mm",
                "width": "286mm",
            };

            pdf.create(content, options).toFile('C:/Users/Administrator/Documents/wscargo/restserver/public/'+mandato, async function(err, res)
            {
                if (err)
                {
                    console.log(".::.");
                    console.log(".::.");
                    console.log(".::.");
                    console.log("Error generar mandato");
                    await client.query(` UPDATE public.clientes SET firma_digital_estado='ERROR MANDATO', firma_digital_mandato=null WHERE id=`+cli_id+` `);
                }
                else
                {
                    try {
                        console.log('\n\nArchivo generado corretacmente ');
                        const newRut = rep_rut.replace(/\./g,'').replace(/\-/g, '').trim().toLowerCase();
                        const lastDigit = newRut.substr(-1, 1);
                        const rutDigit = newRut.substr(0, newRut.length-1)
                        let format = '';
                        for (let i = rutDigit.length; i > 0; i--)
                        {
                            const e = rutDigit.charAt(i-1);
                            format = e.concat(format);
                            if (i % 3 === 0)
                            {
                                format = '.'.concat(format);
                            }
                        }

                        rep_rut = format.concat('-').concat(lastDigit).replace('.', '').replace('.', '').replace('.', '').replace('.', '').trim().toUpperCase();

                        var grupo_nombre = ''+cli_id+' '+cliente_nombre_corto;
                        var documento_nombre = ''+'MANDATO_'+cli_id+'_'+cliente_nombre_corto;

                        let PdfBuff = fs.readFileSync('C:/Users/Administrator/Documents/wscargo/restserver/public/'+mandato);
                        let PdfBuff64 = PdfBuff.toString('base64');
                        console.log('\n\nArchivo cargado en el buffer ');
                        var FirmaYaData = {
                            group_name: grupo_nombre,
                            document_name: documento_nombre ,
                            members: [{ rut:rep_rut, email:rep_email },],
                            notify_users: true,
                            pdf: PdfBuff64
                        };
                        const FirmaYaConfig = {
                            headers: {
                                'Authorization': 'eyJhbGciOiJIUzI1NiJ9.eyJ1c2VyX2lkIjp7IiRvaWQiOiI2MmNjMmEwMmVkZjQxNTc1NmM4ZmM2ZTUifX0.NWvyrYfkYWRbzlrXTAWHRvwhRt4BqIVGsGk4mg7QSCc',
                                'Content-Type': 'application/json',
                            },
                        };

                        console.log('\n\nCreador header de firma ya ');
                        var RequestAxios = await axios.post('https://firmaya.idok.cl/api/corp/groups/create', JSON.parse(JSON.stringify(FirmaYaData)), FirmaYaConfig);
                        console.log(".......");
                        console.log(".......");
                        console.log('\n\nRespuesta firma ya ');
                        console.log(" DETALLE RESPUESTA "+JSON.stringify(RequestAxios.data));

                        if( RequestAxios.status=='200' )
                        {
                            console.log(".::.");
                            console.log(".::.");
                            console.log(".::.");
                            console.log("Mandato cargado");

                            fecha =  new Date();
                            fecha = moment(fecha).format('DD-MM-YYYY');

                            await client.query(`
                            UPDATE
                            public.clientes
                            SET
                            firma_digital_estado='PENDIENTE'
                            , firma_digital_grupo='`+RequestAxios.data.id+`'
                            , firma_digital_mandato='`+mandato+`'
                            , firma_digital_actividad='`+fecha+`'
                            , firma_digital_tipo='FIRMA YA'
                            WHERE
                            id=`+cli_id+`
                            `);

                            if( RequestAxios.data.members.length>0 )
                            {
                                for( var i=0; i<RequestAxios.data.members.length; i++)
                                {
                                    if(RequestAxios.data.members[i].status.toUpperCase()=='ENROLADO')
                                    {
                                        var AuxRutRep = RequestAxios.data.members[0].rut.replace('.', '').replace('.', '').replace('.', '').replace('.', '').replace('-', '').replace('-', '').replace('-', '').trim().toUpperCase();
                                        await client.query(`
                                        UPDATE
                                        public.clientes_contactos
                                        SET
                                        firma_digital_estado='OK'
                                        , firma_digital_id='`+RequestAxios.data.members[0].id+`'
                                        , firma_digital_vigencia=''
                                        WHERE
                                        REPLACE(REPLACE(REPLACE(REPLACE(UPPER(rut),'.',''),'.',''),'.',''),'-','')='`+AuxRutRep+`'
                                        `);
                                    }
                                }
                            }

                            FirmaYaData = { 
                                id: RequestAxios.data.id,
                                notify_users: false,
                            };

                            await axios.post('https://firmaya.idok.cl/api/corp/groups/turn_on_signs', JSON.parse(JSON.stringify(FirmaYaData)), FirmaYaConfig);
                        }
                        else
                        {
                            await client.query(` UPDATE public.clientes SET firma_digital_estado='ERROR MANDATO', firma_digital_mandato=null WHERE id=`+cli_id+` `);
                        }
                    }
                    catch (error)
                    {
                        console.log("\n\nError al cargar mandato "+error);
                        await client.query(` UPDATE public.clientes SET firma_digital_estado='ERROR MANDATO', firma_digital_mandato=null WHERE id=`+cli_id+` `);
                    }
                }
            });
        }
        else
        {
            console.log(".::.");
            console.log(".::.");
            console.log(".::.");
            console.log("Error falta informacion para el mandato ");
            await client.query(` UPDATE public.clientes SET firma_digital_estado='ERROR MANDATO', firma_digital_mandato=null WHERE id=`+cli_id+` `);
        }
    }
}

exports.FirmaDigital_ConsultarEstado = async (fk_cliente) => {

    async function ActualizarRuta(Dato, Ruta, CLienteId)
    {
        client.query(`UPDATE public.clientes SET `+Dato+`='`+Ruta+`' where id=`+CLienteId);
    }

    var Ruta1 = process.env.QiaoRestServerPublic+'files/clientes_archivos/';
    var Ruta2 = 'files/clientes_archivos/';
    var fecha =  new Date();
    var moment = require('moment'); moment.locale('es');
    fecha = moment(fecha).format('DD-MM-YYYY');

    const FirmaYaConfig = {
        headers: {
            'Authorization': 'eyJhbGciOiJIUzI1NiJ9.eyJ1c2VyX2lkIjp7IiRvaWQiOiI2MmNjMmEwMmVkZjQxNTc1NmM4ZmM2ZTUifX0.NWvyrYfkYWRbzlrXTAWHRvwhRt4BqIVGsGk4mg7QSCc',
            'Content-Type': 'application/json',
        },
    };

    var FirmaYaData = { };
    console.log(".::.");
    console.log(".::.");
    console.log(".::.");
    console.log("Consultando estado documentos");

    if(fk_cliente!=null)
    {
        var Condicion = `
        where
        ( coalesce(cli.firma_digital_estado,'')='PENDIENTE' and cli.firma_digital_actividad is null and cli.id=`+fk_cliente+` )
        or ( coalesce(cli.firma_digital_estado,'')='PENDIENTE' and cli.id=`+fk_cliente+` and to_date(cli.firma_digital_actividad, 'DD/MM/YYYY') <= now()::date - INTERVAL '2 day')
        `;
    }
    else
    {
        var Condicion = `
        where
        ( coalesce(cli.firma_digital_estado,'')='PENDIENTE' and cli.firma_digital_actividad is null )
        or ( coalesce(cli.firma_digital_estado,'')='PENDIENTE' and to_date(cli.firma_digital_actividad, 'DD/MM/YYYY') <= now()::date - INTERVAL '2 day')
        `;
    }

    var InfoCliente = await client.query(`
    SELECT
    cli.id as id_ws
    , coalesce(cli.firma_digital_estado,'') as firma_digital_estado
    , coalesce(cli.firma_digital_grupo,'') as firma_digital_grupo
    , coalesce(cli.firma_digital_mandato,'') as firma_digital_mandato
    , cli.firma_digital_vigencia

    FROM
    public.clientes as cli

    `+Condicion+`

    order by
    cli.id
    `);

    console.log(".::.");
    console.log(".::.");
    console.log(".::.");
    console.log("Consultando total grupos "+InfoCliente.rows.length);

    if(InfoCliente && InfoCliente.rows && InfoCliente.rows.length>0)
    {
        const fs = require("fs");

        for(var x=0; x<InfoCliente.rows.length; x++)
        {
            let cli_id      = InfoCliente.rows[x].id_ws;
            let cli_grupo   = InfoCliente.rows[x].firma_digital_grupo;
            console.log(".::.");
            console.log(".::.");
            console.log(".::.");
            console.log("Consultando grupo "+cli_grupo);
            if( cli_grupo.length>=5 )
            {
                try{
                    FirmaYaData = {
                        id: cli_grupo,
                    };
    
                    var RequestAxiosGrupo = await axios.post('https://firmaya.idok.cl/api/corp/groups/show', JSON.parse(JSON.stringify(FirmaYaData)), FirmaYaConfig);
                    console.log(".::.");
                    console.log(".::.");
                    console.log(".::.");
                    console.log(" DETALLE RESPUESTA CONSULTA GRUPO "+JSON.stringify(RequestAxiosGrupo.data));
                }
                catch (err)
                {
                    console.log(".::.");
                    console.log(".::.");
                    console.log(".::.");
                    console.log("ERROR CONSULTA GRUPO "+JSON.stringify(err));
                    await client.query(`
                    UPDATE
                    public.clientes
                    SET
                    firma_digital_estado='ERROR GRUPO'
                    , firma_digital_actividad='`+fecha+`'
                    WHERE
                    id=`+cli_id+`
                    `);
                }
    
                if( typeof RequestAxiosGrupo !== 'undefined' )
                {
                    if( RequestAxiosGrupo.status=='200' )
                    {
                        if( RequestAxiosGrupo.data.signs_allowed!=true )
                        {
                            console.log("ID "+cli_grupo);
                            FirmaYaData = { 
                                id: cli_grupo,
                                notify_users: false,
                            };
    
                            await axios.post('https://firmaya.idok.cl/api/corp/groups/turn_on_signs', JSON.parse(JSON.stringify(FirmaYaData)), FirmaYaConfig);
                        }
    
                        if( RequestAxiosGrupo.data.documents.length>0 )
                        {
                            for( var i=0; i<RequestAxiosGrupo.data.documents.length; i++)
                            {
                                console.log(".......");
                                console.log(".......");
                                console.log(".......");
                                console.log(" CONSULTANDO CICLO "+i);
                                var ExisteDocumento = await client.query(`
                                SELECT
                                *
                                FROM
                                public.clientes_mandatos
                                WHERE
                                fk_cliente=`+cli_id+`
                                and
                                mandato_id='`+RequestAxiosGrupo.data.documents[i].id+`'
                                and
                                coalesce(mandato_estado,'')!='OK'
                                `);
    
                                if ( ExisteDocumento.rows.length<=0)
                                {
                                    await client.query(` 
                                    INSERT INTO
                                    public.clientes_mandatos (fk_cliente, mandato_id, mandato_estado, mandato_vigencia, mandato_ruta, mandato_actividad, mandato_tipo)
                                    values (`+cli_id+`, '`+RequestAxiosGrupo.data.documents[i].id+`', 'PENDIENTE', '', '', '`+fecha+`', 'FIRMA YA')
                                    `);
                                }
    
                                ExisteDocumento = await client.query(`
                                SELECT
                                *
                                FROM
                                public.clientes_mandatos
                                WHERE
                                fk_cliente=`+cli_id+`
                                and
                                mandato_id='`+RequestAxiosGrupo.data.documents[i].id+`'
                                and
                                coalesce(mandato_estado,'')!='OK'
                                `);
    
                                if( ExisteDocumento.rows[0].mandato_estado=='PENDIENTE' )
                                {
                                    FirmaYaData = {
                                        mxml_id: RequestAxiosGrupo.data.documents[i].id,
                                    };
    
                                    var RequestAxiosDocumento = await axios.post('https://firmaya.idok.cl/api/corp/groups/document_status', JSON.parse(JSON.stringify(FirmaYaData)), FirmaYaConfig);
                                    console.log(".......");
                                    console.log(".......");
                                    console.log(".......");
                                    console.log(" DETALLE RESPUESTA CONSULTA ESTADO DOCUMENTO "+JSON.stringify(RequestAxiosDocumento.data));
    
                                    if( RequestAxiosDocumento.data.signers.length>0)
                                    {
                                        console.log(".::.");
                                        console.log(".::.");
                                        console.log(".::.");
                                        console.log("FIRMADO CORRECTAMENTE ID "+RequestAxiosGrupo.data.documents[i].id);

                                        await client.query(`
                                        UPDATE
                                        public.clientes_mandatos
                                        SET
                                        mandato_actividad='`+fecha+`',
                                        mandato_estado='OK',
                                        mandato_firmante_id='`+RequestAxiosDocumento.data.signers[0].id+`',
                                        mandato_firmante_email='`+RequestAxiosDocumento.data.signers[0].email+`'
                                        WHERE
                                        mandato_id='`+RequestAxiosGrupo.data.documents[i].id+`'
                                        `);
    
                                        await client.query(`
                                        UPDATE
                                        public.clientes
                                        SET
                                        firma_digital_actividad='`+fecha+`',
                                        firma_digital_vigencia = TO_CHAR(to_date('`+fecha+`', 'DD/MM/YYYY') + INTERVAL '340 DAY', 'DD/MM/YYYY'),
                                        firma_digital_estado='FIRMADO'
                                        WHERE
                                        id=`+cli_id+`
                                        `);

                                        var Extencion       = '.pdf';
                                        var Nombre1         = cli_id+'_mandato.pdf';
                                        var Nombre2         = cli_id+'_mandato.pdf';
                                        var Data            = null;
                                        var NombreDato      = 'mandato_ruta';

                                        var Cliente_Mandato = RequestAxiosGrupo.data.documents[i].id;

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

                                                ActualizarRuta('mandato_ruta', 'ERROR', cli_id); 
                                            }
                                            else
                                            {
                                                console.log('Saved!');
                                                ActualizarRuta('mandato_ruta', 'files/clientes_archivos/'+Nombre1, cli_id); 
                                            }
                                        });
                                    }
                                    else
                                    {
                                        console.log(".::.");
                                        console.log(".::.");
                                        console.log(".::.");
                                        console.log("NO FIRMADO");
                                        await client.query(`
                                        UPDATE
                                        public.clientes_mandatos
                                        SET
                                        mandato_actividad='`+fecha+`'
                                        WHERE
                                        mandato_id='`+RequestAxiosGrupo.data.documents[i].id+`'
                                        `);
                                    }
                                }
                            }
                        }
    
                    }
                    else
                    {
                        await client.query(`
                        UPDATE
                        public.clientes
                        SET
                        firma_digital_estado='ERROR GRUPO'
                        , firma_digital_actividad='`+fecha+`'
                        WHERE
                        id=`+cli_id+`
                        `);
                    }
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
                id=`+cli_id+`
                `);
            }

            await client.query(`
            UPDATE
            public.clientes
            SET
            firma_digital_actividad='`+fecha+`'
            WHERE
            id=`+cli_id+`
            `);
        }
    }
}

exports.get_comercial_vigente_data = async (Cliente) => {
    return await client.query(`
    SELECT 
    usu.rut,
    usu.usuario,
    usu.nombre,
    usu.apellidos,
    usu.email,
    usu.telefono
    FROM public.usuario as usu
    inner join public.clientes as cli on 
    case when cli.estado_consolidado='SI' and cli.fk_ejecutivocuenta is not null then usu.id=cli.fk_ejecutivocuenta
    else usu.id=cli.fk_comercial end
    where 
    cli.id=`+Cliente+`
    `);

}