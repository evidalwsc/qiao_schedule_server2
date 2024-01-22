/**
 * @desc Integracion de QIAO con ZOHO CRM
 * @param
 * @return
 * @author ARIEL AGUILAR
 * @createAt 01-03-2022
 * @updateAt 04-05-2022
 */
//require("../server/config/config");
const client = require('../server/config/db.client');
const axios = require("axios");
const qs = require("qs");
const QRCode = require("qrcode");
const PDF = require('pdfkit');//Importando la libreria de PDFkit
const path = require('path');
const fs = require('fs');
 
/**
 * 
 * funciones para solicitar informacion a Zoho
 * @returns 
 */

/**
* @desc Funcion para obtener un refresh_toekn
* @param object 
 {
   "name": "TEST EMAIL LIST ",
   "groupType": "Global"
 }
* @return id del EmailList creado
*/
const getRefreshToken = async () => {
  console.log("OBTENGO UN NUEVO REFRESH TOKEN");
  const options = {
    method: "POST",
    url: `${process.env.ZOHO_AUTH}/token`,
    headers: {
      "Content-type": "application/x-www-form-urlencoded",
    },
    data: qs.stringify({
      code: process.env.ZOHO_REFRESH_TOKEN,
      redirect_uri: "http://example.wscargo.cl",
      client_id: process.env.ZOHO_ClIENT_ID,
      client_secret: process.env.ZOHO_ClIENT_SECRET,
      grant_type: "authorization_code",
    }),
  };
  try {
    const response = await axios(options);
    //console.log(response.data);
    const { data } = response.data;
    return data;
  } catch (e) {
    console.log("ha ocurrido un error en getRefreshToken " + e.message);
  }
};
/**
 * @desc Funcion para obtener un access_token 
 * @param object 
    {
        refresh_token: process.env.ZOHO_REFRESH_TOKEN,
        client_id: process.env.ZOHO_ClIENT_ID,
        client_secret: process.env.ZOHO_ClIENT_SECRET,
        grant_type: "refresh_token"
    }
 * @return token
 */
const getAccessToken = async () => {
  //console.log("OBTENGO UN NUEVO TOKEN");
  const options = {
    method: "POST",
    url: `${process.env.ZOHO_AUTH}/token`,
    headers: {
      "Content-type": "application/x-www-form-urlencoded",
    },
    data: qs.stringify({
      refresh_token: process.env.ZOHO_REFRESH_TOKEN,
      client_id: process.env.ZOHO_ClIENT_ID,
      client_secret: process.env.ZOHO_ClIENT_SECRET,
      grant_type: "refresh_token",
    }),
  };
  try {
    const response = await axios(options);
    const { access_token } = response.data;
    return access_token;
  } catch (e) {
    console.log("ha ocurrido un error en getAccessToken " + e.message);
  }
};
const createInZoho = async (endpoint, payload, access_token) => {
  console.log("SE CREA UN "+endpoint);
  const options = {
    method: "POST",
    url: `${process.env.ZOHO_CRM}/${endpoint}`,
    headers: {
      Authorization: "Zoho-oauthtoken " + access_token,
      "Content-type": "application/json",
    },
    //data: { data : payload, trigger: [ "approval", "workflow", "blueprint" ] },
    data: { data : payload, trigger: [ ] },
  };
  try {
    const data = await axios(options);
    //console.log(data.data);
    return data.data;
  } catch (e) {
    console.log("ha ocurrido un error en createInZoho " + e.message);
  }
};
const updateInZoho = async (endpoint, payload, access_token) => {
  console.log("SE ACTUALIZA "+endpoint);
  const options = {
    method: "PUT",
    url: `${process.env.ZOHO_CRM}/${endpoint}`,
    headers: {
      Authorization: "Zoho-oauthtoken " + access_token,
      "Content-type": "application/json",
    },
    //data: { data : payload, trigger: [ "approval", "workflow", "blueprint" ] },
    data: { data : payload, trigger: [] },
  };
  try {
    const res = await axios(options);
    return res.data;
  } catch (e) {
    console.log("ha ocurrido un error en updateInZoho " + e.message);
  }
};
const deleteContact = async (id, access_token) => {
    console.log("SE ELIMINA CONTACT");
    const options = {
      method: "DELETE",
      url: `${process.env.ZOHO_CRM}/Contacts/${id}`,
      headers: {
            Authorization: "Zoho-oauthtoken " + access_token,
      },
      searchParams:{
            wf_trigger: true
      },
      throwHttpErrors : false
    };
    try {
      const data = await axios(options);
      //console.log(data.data);
      return data;
    } catch (e) {
      console.log("ha ocurrido un error en deleteContact " + e.message);
    }
};
const getIdUsersZoho = async (email , access_token) => {
  console.log("Obtener usuario Zoho");
  const options = {
    method: "GET",
    url: `${process.env.ZOHO_CRM}/users/search?criteria=((email:equals:${email}))`,
    headers: {
      Authorization: "Zoho-oauthtoken " + access_token,
    },
  };
  try {
    const data = await axios(options);
    let id;
    if(data.status === 200 && data.data !== ''){
      const { users } = data.data;
      id = users[0].id;
    }
    return id;   
  } catch (e) {
    console.log("ha ocurrido un error en getIdUsersZoho " + e.message);
  }
};
const getBuscarCuenta = async (access_token, id) => {
  //console.log("Obtener Cuenta");
  const options = {
    method: "GET",
    url: `${process.env.ZOHO_CRM}/Accounts/search?criteria=((Account_Number:equals:${id}))`,
    headers: {
      Authorization: "Zoho-oauthtoken " + access_token,
    },
  };
  try {
    const response = await axios(options);
    let cuenta; 
    if( response.status === 200 && response.data !== '' ){
      cuenta = response.data.data[0]; 
    }
    
    return cuenta;
  } catch (e) {
    console.log("ha ocurrido un error en getBuscarCuenta " + e.message);
  }
};
const getBuscarContact = async (access_token, email) => {
  console.log("Obtener Contact");
  const options = {
    method: "GET",
    url: `${process.env.ZOHO_CRM}/Contacts/search?criteria=((Email:equals:${email}))`,
    headers: {
      Authorization: "Zoho-oauthtoken " + access_token,
    },
  };
  try {
    const response = await axios(options);
    let contacto ;
    if(response.status === 200 && response.data !== ''){
      contacto = response.data.data[0];
    }
    
    return contacto;
  } catch (e) {
    console.log("ha ocurrido un error en getBuscarContacto " + e.message);
  }
};
const getBuscarPotentials = async (access_token, email) => {
  console.log("Obtener Potential");
  const options = {
    method: "GET",
    url: `${process.env.ZOHO_CRM}/Potentials/search?criteria=((Email:equals:${email}))`,
    headers: {
      Authorization: "Zoho-oauthtoken " + access_token,
    },
  };
  try {
    const response = await axios(options)
    const contacto = response.data.data[0];
    return contacto;
  } catch (e) {
    console.log("ha ocurrido un error en getBuscarPotentials " + e.message);
  }
};
const getCuentas = async (access_token) => {
    console.log("OBTENER CUENTAS");
    const options = {
      method: "GET",
      url: `${process.env.ZOHO_CRM}/Accounts`,
      headers: {
        Authorization: "Zoho-oauthtoken " + access_token,
      },
    };
    try {
      const data = await axios(options);
      //console.log(data.data);
      return data;
    } catch (e) {
      console.log("ha ocurrido un error en getCuentas " + e.message);
    }
};

/**
 * 
 * funciones para solicitar informacion a base de datos
 * @returns 
 */
const getPropuestaComercial = async (id) => {
  try {
    let sql = `
    select  
      pc.id as "idPropuestaComercial",
      pc."idPotential",
      (SELECT email FROM public.gc_registrocontactos rc  WHERE rc.id = pc.fk_contacto ) AS "emailContacto",
      (SELECT c.id FROM public.clientes c WHERE c.id = pc.fk_cliente) AS "numberAccount",
      CASE WHEN (( select email from public.usuario where id = (Select fk_comercial from public.gc_registrocontactos rc  where rc.id = pc.fk_contacto )) IS NULL ) THEN 
          (select email from public.usuario where id = (Select fk_comercial from public.clientes c  where c.id = pc.fk_cliente ))
          ELSE
            ( select email from public.usuario where id = (Select fk_comercial from public.gc_registrocontactos rc  where rc.id = pc.fk_contacto ) )
          END 
        AS "ejecutivo"
    from public.gc_propuestas_cabeceras pc
    where id = ${id} ;`;

    let result = await client.query(sql);

    return result.rows[0];
  } catch (e) {
    console.log("Error en getPropuestaComercial :" + e.message);
  }
};
const getTarifa = async (id) => {
  try {
    let sql = `
      select 
        sum(t."pesoEstimado") AS "pesoEstimado" ,
        sum(t."volumenEstimado") AS "volumenEstimado",
        sum(t."unidadesACobrar") AS "unidadesACobrar",
        sum(t."tarifaUsd") AS "tarifaUsd"
      from gc_propuestas_tarifas t
      where t.fk_cabecera = ${id}
      and estado = true
      group by t.fk_cabecera
    `;

    let result = await client.query(sql);
    if(result.rows.length > 0 ){
      return result.rows[0];
    }else{
      return undefined;
    }
  } catch (e) {
    console.log("Error en getTarifa :" + e.message);
  
  }
};
const getFkCabeceraByIdTarifa = async (id) => {
  try {
    let sql = ` SELECT 
                  fk_cabecera,
                  (SELECT "idPotential" FROM public.gc_propuestas_cabeceras WHERE fk_cabecera = id) as "idPotential"
                FROM gc_propuestas_tarifas 
                WHERE id = ${id} ;`;

    let result = await client.query(sql);
    if(result.rows.length > 0 ){
      return result.rows[0];
    }else{
      return undefined;
    }
  } catch (e) {
    console.log("Error en getFkCabeceraByIdTarifa :" + e.message);
  
  }
};
const getContactoById = async (id) => {
  try {
      let sql = `
          select 
              (select email from public.usuario where id = c.fk_comercial) as ejecutivo,
              c."razonSocial",
              cc.email,
              cc.telefono_1,
              cc.telefono_2,
              cc.nombre,
              cc.apellido,
              cc.cargo
          from public.clientes_contactos cc
          inner join public.clientes c on c.id = cc.fk_cliente
          where cc.id = '${id}' `;

      let result = await client.query(sql);

      return result.rows[0];
  } catch (e) {
    console.log("Error en getContactoById :" + e.message);
  }
};
const getFkComercial = async (email) => {
  try {
    let sql = `
        SELECT id
        FROM public.usuario 
        WHERE email = '${email}'`;
    let result = await client.query(sql);

    return result.rows[0].id;
  } catch (e) {
    console.log("Error en getFkComercial :" + e.message);
  }
}
const getFkComercialByFkCliente = async (fk_cliente) => {
  try {
    let sql = `
        SELECT fk_comercial
        FROM public.clientes 
        WHERE id = '${fk_cliente}';`;
    let result = await client.query(sql);

    return result.rows[0].fk_comercial;
  } catch (e) {
    console.log("Error en getFkComercialByFkCliente :" + e.message);
  }
}
const getEmailComercial = async (fk_comercial) => {
  
  try {

      const sql = {
              text:`select email from public.usuario where id = $1`,
              values:[fk_comercial]
      };

      let result = await client.query(sql);
      //console.log(result);
      return result.rows[0].email;
  } catch (e) {
    console.log("Error en getEmailComercial :" + e.message);
  }
}
const getFkContacto = async (email) => {
  try {
    let sql = `
        SELECT id
        FROM public.gc_registrocontactos 
        WHERE email = '${email.toUpperCase().trim()}'`;

    let result = await client.query(sql);
    return result.rows[0].id;
  } catch (e) {
    console.log("Error en getFkContacto :" + e.message);
  }
}
const putIdPotential = async (idPropuestaComercial, idCotizacion) => {
  try {

      const sql = {
              text:`UPDATE public.gc_propuestas_cabeceras
                      SET "idPotential" = $1
                      WHERE id = $2 ;`,
              values:[idCotizacion, idPropuestaComercial]
      };

      let result = await client.query(sql);
      return result.rows[0];
  } catch (e) {
    console.log("Error en putIdCotizacion :" + e.message);
  }
};
const getClienteContacto = async (fk_cliente, email) => {
  try {

      const sql = {
              text:`SELECT 
                      fk_cliente,
                      nombre,
                      apellido,
                      telefono_1,
                      telefono_2,
                      email,
                      cargo
                      FROM public.clientes_contactos 
                      WHERE fk_cliente=$1 AND UPPER(email)=UPPER($2) AND fk_tipo=1`,
              values:[fk_cliente,email]
      };

      let result = await client.query(sql);
      //console.log(result);
      return result.rows[0];
  } catch (e) {
    console.log("Error en getContacto :" + e.message);
  }
};
const getCliente = async (id) => {
  try {
    let sql = `
    select 
    cl.id, 
    cl.giro,
    cl.telefono1, 
    cl.telefono2,
    cl.rut, 
    cl."dteEmail", 
    cl.codigo, 
    cl."codigoSii",
    cl."razonSocial", 
    (select email from public.usuario where id = cl.fk_comercial) as ejecutivo,
    (select nombre from public.clientes_direcciones where fk_cliente = cl.id AND fk_tipo = 1 limit 1),
    'CHILE' as pais,
    ( select CONCAT(c.id,'-',c.codigo,'-',c.nombre) 
      from public.comunas c
      where c.id = (select fk_comuna from public.clientes_direcciones where fk_cliente = cl.id AND fk_tipo = 1 limit 1)) as comuna,
    ( select CONCAT(r.id,'-',r.codigo,'-',r.nombre) 
      from public.region r
      where r.id = (select fk_region from public.clientes_direcciones where fk_cliente = cl.id AND fk_tipo = 1 limit 1)) as region,
    (select direccion from public.clientes_direcciones where fk_cliente = cl.id AND fk_tipo = 1 limit 1),
    (select numero from public.clientes_direcciones where fk_cliente = cl.id AND fk_tipo = 1 limit 1)
    from public.clientes cl 
    where cl.id = ${id} ;`;

    let result = await client.query(sql);

    return result.rows[0];
  } catch (e) {
    console.log("Error en getCliente :" + e.message);
  }
};
const getRepLegal = async (fk_cliente) => {
  try {
    let sql = `
    select 
    nombre as "NombreRepLegal",
	  apellido as "ApellidoRepLegal",
	  rut as "rutRepLegal",
	  email as "emailRepLegal",
	  telefono_1 as "telefonoRepLegal"
    from public.clientes_contactos 
    where fk_cliente = ${fk_cliente} 
    AND fk_tipo = 4 limit 1 ;`;

    let result = await client.query(sql);

    if(result && result.rows && result.rows.length > 0){
      return result.rows[0];
    }else{
      return { 
        NombreRepLegal:'',
        ApellidoRepLegal:'',
        rutRepLegal:'',
        emailRepLegal:'',
        telefonoRepLegal:'' 
      };
    }
  } catch (e) {
    console.log("Error en getRepLegal :" + e.message);
  }
}
const getRegistroContacto = async (email) => {
  try {
    let sql = `
        SELECT 
            (select email from public.usuario where id = fk_comercial) as ejecutivo,
            email,
            telefono1,
            telefono2,
            nombres,
            apellidos,
            texto
        FROM public.gc_registrocontactos 
        WHERE UPPER(email) = UPPER('${email}') `;

    let result = await client.query(sql);
    return result.rows[0];
  } catch (e) {
    console.log("Error en getRegistroContacto :" + e.message);
  }
};
const getRegistroContactoById = async (fk_contacto) => {
  try {
    let sql = `
        SELECT 
            (select email from public.usuario where id = fk_comercial) as ejecutivo,
            email,
            telefono1,
            telefono2,
            nombres,
            apellidos,
            texto
        FROM public.gc_registrocontactos 
        WHERE id = '${fk_contacto}' `;

    let result = await client.query(sql);
    return result.rows[0];
  } catch (e) {
    console.log("Error en getRegistroContactoById :" + e.message);
  }
};
const getPropuestaBase = async () => {
  try {
    const sql = `
                SELECT
                *
                FROM public.gc_propuestas_tarifas
                WHERE
                estado=true
                and fk_cabecera=1
                and "fk_zonaOrigen" = 2
                and "fk_zonaAlmacenaje" = 1 
                and "fk_zonaDestino" = 1
                LIMIT 1;
              `;
    const propuestaBase = await client.query(sql);
    return propuestaBase.rows[0];

  } catch (error) {
      console.log("ERROR en getPropuestaBase :" +error.message);
  }
}
const getInformationEtiqueta = async (id) => {
  try{
    let sql = `
          SELECT
            cli.id
            , cli."razonSocial"
            , cli.rut
            , '' as direccion
            , cli.telefono1
            , cli.codigo
            , COALESCE(CONCAT(dir.direccion,' ',dir.numero,', ',comunas.nombre),'') as direccion
            , (select CONCAT(nombre,' ',apellidos) FROM public.usuario WHERE id = cli.fk_comercial ) as comercial
          from public.clientes as cli
          inner join public.clientes_direcciones as dir on cli.id=dir.fk_cliente
          inner join direcciones_tipos as dir_tipo on dir_tipo.id=dir.fk_tipo
          inner join pais on pais.id=dir.fk_pais
          inner join region on region.id=dir.fk_region
          inner join comunas on comunas.id=dir.fk_comuna
          where
          cli.id=`+id+`
          limit 1
        `;
      var InfoQr = await client.query(sql);
      return InfoQr;

  } catch (error) {
      console.log("ERROR en getInformationEtiqueta :" +error.message);
  }
}

/**
 * 
 * funciones para crear payloads para Zoho
 * @returns 
 */
const createDataCuenta = (
  { id, giro, telefono1, telefono2, rut, dteEmail, codigo, codigoSii, razonSocial, nombre, pais, region, comuna, direccion, numero },
  { NombreRepLegal,
	  ApellidoRepLegal,
	  rutRepLegal,
	  emailRepLegal,
	  telefonoRepLegal },
  ejecutivo
) => {
  let data = [];
  try {
    data.push({
      Owner: ejecutivo.id,
      Account_Name: razonSocial,
      Account_Number: id,
      Giro: giro,
      Phone: telefono1,
      Telefono_Secundario: telefono2,
      rut: rut,
      dteEmail: dteEmail,
      codigoSii: codigoSii,
      codigo: codigo,
      Nombre: nombre,
      Pais: pais,
      Region: region,
      Comuna: comuna,
      Direccion: direccion,
      Numero: numero,
      Criterio:'API',
      Layout:process.env.LAYOUTID_ACCOUNTS,
      Nombre_Representante: NombreRepLegal ,
      Apellido_Representante: ApellidoRepLegal,
      Rut_Representante: rutRepLegal ,
      Telefono_Representante: telefonoRepLegal,
      Email_Representante: emailRepLegal 
    });
    return data;
  } catch (e) {
    console.log("Ha ocurrido un error en createDataCuenta : " + e.message);
  }
};
const createDataCuentaUpdate = (
  { id, giro, telefono1, telefono2, rut, dteEmail, codigo, codigoSii, razonSocial, nombre, pais, region, comuna, direccion, numero } 
  , { 
    NombreRepLegal,
    ApellidoRepLegal,
    rutRepLegal,
    emailRepLegal,
    telefonoRepLegal 
  },
  ejecutivo,
  cuenta
) => {
  let data = [];
  try {
    data.push({
      id: cuenta.id,
      Owner: ejecutivo.id,
      Account_Name: razonSocial,
      Account_Number: id,
      Giro: giro,
      Phone: telefono1,
      Telefono_Secundario: telefono2,
      rut: rut,
      dteEmail: dteEmail,
      codigo: codigo,
      codigoSii:codigoSii,
      Nombre: nombre,
      Pais: pais,
      Region: region,
      Comuna: comuna,
      Direccion: direccion,
      Numero: numero,
      Criterio:'API',
      Layout:process.env.LAYOUTID_ACCOUNTS,
      Nombre_Representante: NombreRepLegal ,
      Apellido_Representante: ApellidoRepLegal,
      Rut_Representante: rutRepLegal ,
      Telefono_Representante: telefonoRepLegal,
      Email_Representante: emailRepLegal 
    });
    
    return data;
  } catch (e) {
    console.log("Ha ocurrido un error en createDataCuentaUpdate : " + e.message);
  }
};
const createDataContacto = ({ razonSocial, email, telefono_1, telefono_2, nombre, apellido, cargo, texto },ejecutivo) => {
  let data = [];
  try {
    data.push({
      Owner: ejecutivo.id,
      Account_Name: razonSocial,
      Email: email,
      Phone: telefono_1,
      Mobile: telefono_2,
      First_Name: nombre,
      Last_Name: apellido,
      Title: cargo,
      Description: texto,
      Layout:process.env.LAYOUTID_CONTACT
    });
    return data;
  } catch (e) {
    console.log("Ha ocurrido un error en createDataContacto : " + e.message);
  }
};
const createDataContactoUpdate = (
  { razonSocial, email, telefono_1, telefono_2, nombre, apellido, cargo, texto },
  ejecutivo,
  contact
) => {
  let data = [];
  try {
    data.push({
      id: contact.id,
      Owner: ejecutivo.id,
      Account_Name: razonSocial,
      Email: email,
      Phone: telefono_1,
      Mobile: telefono_2,
      First_Name: nombre,
      Last_Name: apellido,
      Title: cargo,
      Description: texto,
      //Layout:process.env.LAYOUTID_CONTACT
    });
    return data;
  } catch (e) {
    console.log("Ha ocurrido un error en createDataContactoUpdate : " + e.message);
  }
};
const createDataPotential = ( idCotizacion, idPropuestaComercial, peso, volumen, tarifa, total, idCase, idContacto, idAccount, ejecutivo) => {
  let data = [];
  idCotizacion = idCotizacion == null ? '': idCotizacion;
  try {
    data.push({
      Deal_Name: `Cotizacion - ${idCotizacion} - ${idPropuestaComercial} `,
      Pipeline : "Ciclo de Venta CargoWS",
      Stage: "Clasificación",
      IdCotizacion :  idCotizacion ,
      IdPropuestaComercial : `${idPropuestaComercial}`,
      Peso: peso,
      Volumen: volumen,
      Tarifa: tarifa,
      Total: total,
      Amount: total,
      Tipo_Moneda_1: "CLP",
      Casos: idCase,
      Contact_Name: idContacto ,
      Cliente: idAccount,
      Owner: ejecutivo,
      Layout: process.env.LAYOUTID_POTENTIALS
  });
    return data;
  } catch (e) {
    console.log("Ha ocurrido un error en createDataCotizacion : " + e.message);
  }
};
const createDataPotentialUpdate = ( idCotizacion, idPotential, tarifa, propuestaBase) => {
  let data = [];
  idCotizacion = idCotizacion == null ? '': idCotizacion;
  try {
    data.push({
      id: idPotential,
      Peso: tarifa.pesoEstimado.toFixed(14),
      Volumen: tarifa.volumenEstimado.toFixed(14),
      Total: tarifa.tarifaUsd,
      Amount: tarifa.tarifaUsd,
      valorBaseUsd: propuestaBase.valorBaseUsd,
      unidadesACobrar: tarifa.unidadesACobrar,
      valorUnitarioUsd: propuestaBase.valorUnitarioUsd,
      cmbPeso: propuestaBase.cmbPeso,
      Description: propuestaBase.tipoDeCarga
  });
    return data;
  } catch (e) {
    console.log("Ha ocurrido un error en createDataPotentialUpdate : " + e.message);
  }
};
const createDataCase = ( idCase, status) => {
  let data = [];
  try {
    data.push({
      "id": `${idCase}`,
      "Status" : `${status}`,
  });
    return data;
  } catch (e) {
    console.log("Ha ocurrido un error en createDataCase : " + e.message);
  }
};

/**
 * 
 * @description funciones utilitarias 
 * 
 */
const calcularTarifa = (peso, volumen, propuestaBase) => {
  var volumenEstimado = volumen;
  var pesoEstimado = peso;
  var valorUnitarioUsd = propuestaBase.valorUnitarioUsd;
  var valorBaseUsd = propuestaBase.valorBaseUsd;
  var cmbPeso = propuestaBase.cmbPeso;
  //var Pb_unidadesACobrar = propuestaBase.Pb_unidadesACobrar;
  var Pb_unidadesACobrar = propuestaBase.Pb_unidadesACobrar <=0 ? propuestaBase.unidadesACobrar:propuestaBase.Pb_unidadesACobrar;
  if(!volumenEstimado) { volumenEstimado=0; }
  if(!pesoEstimado) { pesoEstimado=0; }
  if(!valorUnitarioUsd) { valorUnitarioUsd=0; }
  if(!valorBaseUsd) { valorBaseUsd=0; }

  var Peso = Math.round(parseFloat(pesoEstimado)/parseFloat(cmbPeso) * 100) / 100;

  if(Number(volumenEstimado)==Number(Peso)) { var Unidades = volumenEstimado; }
  else if(Number(volumenEstimado)>Number(Peso)) { var Unidades = volumenEstimado; }
  else if(Number(Peso)>Number(volumenEstimado)) { var Unidades = Peso; }
  else { var Unidades = 0; }

  if(Number(Unidades)<1) { Unidades=1; }

  var tarifaUsd = Number(Unidades)*parseFloat(valorUnitarioUsd);           

  if(Number(Unidades)<Number(Pb_unidadesACobrar)) { tarifaUsd = Number(tarifaUsd)+Number(valorBaseUsd); }
  tarifaUsd = Math.round(tarifaUsd);
  return { tarifaUsd, Unidades};
  //return tarifaUsd;
}
const generateQRCodeImage = async (id) => {
  const pathQr = path.resolve('./server/controllers/etiquetas/'+id+'.png');
  const codeQR ='wsc_'+id;
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
const procesarNombre = (nombres) => {
  nombres = nombres.split(' ');
  //console.log(nombres[0], nombres[1], nombres[2], nombres[3]);
  var apellidos = "";
  if(nombres.length == 1){
    apellidos = nombres[0];
    nombres = nombres[0];
  }else if(nombres.length == 2){
    apellidos = nombres[1];
    nombres = nombres[0];
  }else if(nombres.length == 3){
    apellidos = nombres[1]+' '+nombres[2];
    nombres = nombres[0];
  }else if(nombres.length >= 4){
    apellidos = nombres[2]+' '+nombres[3];
    nombres = nombres[0]+' '+nombres[1];
  }

  return {
    nombres,
    apellidos
  };
}

/**
 * 
 * @description funciones de procesos de carga o envio de informacion
 * 
 */

const SendCreateRegistroContacto = async (email, fk_comercial) => {
  const registro_contacto = await getRegistroContacto(email);
  const emailComercial = await getEmailComercial(fk_comercial);
  const access_token = await getAccessToken();
  const idPropietario = await getIdUsersZoho(emailComercial, access_token);
  
  if(typeof idPropietario !== 'undefined'){
    let contacto =  { 
      razonSocial: null, 
      email: registro_contacto.email, 
      telefono_1: registro_contacto.telefono1, 
      telefono_2: registro_contacto.telefono2, 
      nombre:registro_contacto.nombres, 
      apellido: registro_contacto.apellidos, 
      cargo:null,
      texto: registro_contacto.texto
    }
    let propietario = {
      id: idPropietario
    }
    const data = createDataContacto(contacto, propietario);
    const response = await createInZoho("Contacts", data, access_token);
    console.log(response.data);
  }
};
const SendUpdateRegistroContacto = async (email, fk_comercial) => {
  const emailComercial = await getEmailComercial(fk_comercial);
  const registro_contacto = await getRegistroContacto(email);
  const access_token = await getAccessToken();
  const idPropietario = await getIdUsersZoho(emailComercial, access_token);
  const contact = await getBuscarContact(access_token,email);
  if(typeof idPropietario !== 'undefined' && typeof contact !== 'undefined' ){
    let contacto =  { 
      razonSocial: null, 
      email: registro_contacto.email, 
      telefono_1: registro_contacto.telefono1, 
      telefono_2: registro_contacto.telefono2, 
      nombre:registro_contacto.nombres, 
      apellido: registro_contacto.apellidos, 
      cargo:null,
      texto:registro_contacto.texto
    }
    let propietario = {
      id: idPropietario
    }
    const data = createDataContactoUpdate(contacto, propietario, contact);
    const response = await updateInZoho("Contacts", data, access_token);
    console.log("Zoho responde",response.data);
  }
};
const SendCreateCliente = async (id) => {
  const cliente = await getCliente(id);
  const representante = await getRepLegal(id);
  const access_token = await getAccessToken();
  const idPropietario = await getIdUsersZoho(cliente.ejecutivo, access_token);

  if(typeof idPropietario !== 'undefined' ){
    let propietario = {
      id: idPropietario
    }
    const data = createDataCuenta(cliente, representante, propietario);
    const response = await createInZoho("Accounts",data, access_token);
    console.log("Zoho responde",response.data);
  }
};
const SendUpdateCliente = async (id) => {
  const cliente = await getCliente(id);
  const representante = await getRepLegal(id);
  const access_token = await getAccessToken();
  const account = await getBuscarCuenta(access_token, id )
  const idPropietario = await getIdUsersZoho(cliente.ejecutivo, access_token);
  if( (typeof account !== 'undefined') && (typeof idPropietario !== 'undefined') ){
    let propietario = {
      id: idPropietario
    }
    const data = createDataCuentaUpdate(cliente, representante, propietario, account);
    const response = await updateInZoho("Accounts",data, access_token);
    console.log("Zoho responde",response.data);
  }
};
const SendCreateCotizacionFromRegistroContacto = async (id) => {
  const propuestaComercial = await getPropuestaComercial(id);
  const access_token = await getAccessToken();
  const contacto = await getBuscarContact(access_token, propuestaComercial.emailContacto);
  const idPropietario = await getIdUsersZoho(propuestaComercial.ejecutivo, access_token);
  if(typeof contacto !== 'undefined' && typeof idPropietario !== 'undefined'){
    const data =  createDataPotential(null, id, null, null, null, null, null, contacto.id, null,idPropietario );
    const response = await createInZoho("Potentials",data, access_token);
    console.log("Zoho responde",response.data);
    await putIdPotential(id, response.data[0].details.id);
  }
};
const SendCreateCotizacionFromPropuestaComercial = async (fk_cabecera) => {
  const propuestaComercial = await getPropuestaComercial(fk_cabecera);
  
  if( (propuestaComercial.emailContacto !== null) && (propuestaComercial.numberAccount !== null) ){
    //viene asociado a un cliente
    const access_token = await getAccessToken();
    //obtengo el account de zoho
    const account = await getBuscarCuenta(access_token,propuestaComercial.numberAccount );
    //obtengo el id del propietario
    const idPropietario = await getIdUsersZoho(propuestaComercial.ejecutivo, access_token);
    //busco el contacto con el email del account en los contactos y lo obtengo
    const contacto = await getBuscarContact(access_token, propuestaComercial.emailContacto);
    if( (typeof account !== 'undefined') && (typeof contacto  !== 'undefined' ) && (typeof idPropietario !== 'undefined' )){
      //creo el payload
      const data =  createDataPotential(null, fk_cabecera, null, null, null, null, null, contacto.id, account.id ,idPropietario );
      console.log(data);
      //lo creo en zoho
      const response = await createInZoho("Potentials",data, access_token);
      console.log("Zoho responde",response.data);
      //actualizo el idpotential del fk_cabecera en qiao
      await putIdPotential(fk_cabecera, response.data[0].details.id);
    }
  }else if( (propuestaComercial.emailContacto === null)  && (propuestaComercial.numberAccount !== null) ){
    //viene asociado a un cliente
    const access_token = await getAccessToken();
    //obtengo el account de zoho
    const account = await getBuscarCuenta(access_token,propuestaComercial.numberAccount );

    if(typeof account !== 'undefined'){
      //busco el contacto con el email del account en los contactos y lo obtengo
      const contacto = await getBuscarContact(access_token, account.dteEmail);
      //obtengo el id del propietario
      const idPropietario = await getIdUsersZoho(propuestaComercial.ejecutivo, access_token);
      if( (typeof contacto  !== 'undefined' ) && (typeof idPropietario !== 'undefined' )){
        //creo el payload
        const data =  createDataPotential(null, fk_cabecera, null, null, null, null, null, contacto.id, account.id ,idPropietario );
        //lo creo en zoho
        const response = await createInZoho("Potentials",data, access_token);
        console.log("Zoho responde",response);
        //actualizo el idpotential del fk_cabecera en qiao
        await putIdPotential(fk_cabecera, response.data[0].details.id);
      }
    }
  }else if(propuestaComercial.emailContacto !== null  && propuestaComercial.numberAccount === null){
    //viene asociado a un contacto lo creo de la misma forma que desde el registro contacto
    await SendCreateCotizacionFromRegistroContacto(fk_cabecera);
  }
};
const SendUpdateCotizacionFromTarifas = async (fk_cabecera) => {
  const propuestaComercial = await getPropuestaComercial(fk_cabecera);
  const propuestaBase = await getPropuestaBase();
  let tarifa = await getTarifa(fk_cabecera);
  let data = {};
  if(propuestaComercial.idPotential === null){
    if(propuestaComercial.emailContacto !== null && propuestaComercial.numberAccount !== null){
      //viene asociado a un cliente
      const access_token = await getAccessToken();
      //obtengo el account de zoho
      const account = await getBuscarCuenta(access_token,propuestaComercial.numberAccount );
      //busco el contacto con el email del account en los contactos y lo obtengo
      const contacto = await getBuscarContact(access_token, propuestaComercial.emailContacto);
      //obtengo el id del propietario
      const idPropietario = await getIdUsersZoho(propuestaComercial.ejecutivo, access_token);

      if( (typeof account !== 'undefined') && (typeof contacto  !== 'undefined' ) && (typeof idPropietario !== 'undefined' )){
        //creo el payload
        data =  createDataPotential(null, fk_cabecera, null, null, null, null, null, contacto.id, account.id ,idPropietario );
        //lo creo en zoho
        let response = await createInZoho("Potentials",data, access_token);
        console.log("Zoho responde",response);
        //actualizo el idpotential del fk_cabecera en qiao
        await putIdPotential(fk_cabecera, response.data[0].details.id);
        //actualizo en crm
        data =  createDataPotentialUpdate(null, response.data[0].details.id, tarifa, propuestaBase);
        response = await updateInZoho("Potentials",data, access_token);
        console.log("Zoho responde",response);
      }      
    }else if(propuestaComercial.emailContacto === null  && propuestaComercial.numberAccount !== null){
      //viene asociado a un cliente
      const access_token = await getAccessToken();
      //obtengo el account de zoho
      const account = await getBuscarCuenta(access_token,propuestaComercial.numberAccount );

      if( typeof account !== 'undefined'){
        //busco el contacto con el email del account en los contactos y lo obtengo
        const contacto = await getBuscarContact(access_token, account.dteEmail);
        //obtengo el id del propietario
        const idPropietario = await getIdUsersZoho(propuestaComercial.ejecutivo, access_token);
        if( (typeof account !== 'undefined') && (typeof contacto  !== 'undefined' ) && (typeof idPropietario !== 'undefined' )){
          //creo el payload
          data =  createDataPotential(null, fk_cabecera, null, null, null, null, null, contacto.id, account.id ,idPropietario );
          //lo creo en zoho
          let response = await createInZoho("Potentials",data, access_token);
          console.log("Zoho responde",response.data);
          //actualizo el idpotential del fk_cabecera en qiao
          await putIdPotential(fk_cabecera, response.data[0].details.id);
          //actualizo en crm
          tarifa = await getTarifa(fk_cabecera);
          data =  createDataPotentialUpdate(null, response.data[0].details.id, tarifa, propuestaBase);
          response = await updateInZoho("Potentials",data, access_token);
          console.log("Zoho responde", response.data);
        }   
      }
    }else if(propuestaComercial.emailContacto !== null  && propuestaComercial.numberAccount === null){
      //viene asociado a un contacto lo creo de la misma forma que desde el registro contacto
      const access_token = await getAccessToken();
      const contacto = await getBuscarContact(access_token, propuestaComercial.emailContacto);
      const idPropietario = await getIdUsersZoho(propuestaComercial.ejecutivo, access_token);
      if( (typeof contacto !== 'undefined') && (typeof idPropietario !== 'undefined') ){
        data =  createDataPotential(null, fk_cabecera, null, null, null, null, null, contacto.id, null, idPropietario );
        let response = await createInZoho("Potentials",data, access_token);
        console.log("Zoho responde",response.data);
        await putIdPotential(fk_cabecera, response.data[0].details.id);
        tarifa = await getTarifa(fk_cabecera);
        data =  createDataPotentialUpdate(null, response.data[0].details.id, tarifa, propuestaBase);
        response = await updateInZoho("Potentials",data, access_token);
        console.log("Zoho responde", response.data);
      }
    }else{
      console.log("No cumple con las condiciones");
    }
  }else{
    data =  createDataPotentialUpdate(null, propuestaComercial.idPotential, tarifa, propuestaBase);
    const access_token = await getAccessToken();
    const response = await updateInZoho("Potentials",data, access_token);
    console.log("Zoho responde:",response.data);
  }
};
const SendUpdateCotizacionFromDeleteTarifa = async (idTarifa) => {
  const cabecera = await getFkCabeceraByIdTarifa(idTarifa);
  const propuestaBase = await getPropuestaBase();
  const tarifa = await getTarifa(cabecera.fk_cabecera);
  const propuestaComercial = await getPropuestaComercial(cabecera.fk_cabecera);
  let data = {};
  let access_token = "";
  let response = {};
  console.log(tarifa);
  if(typeof tarifa !== 'undefined'){
    if(propuestaComercial.idPotential != null ){
      data =  createDataPotentialUpdate(null, propuestaComercial.idPotential, tarifa, propuestaBase);
      access_token = await getAccessToken();
      response = await updateInZoho("Potentials",data, access_token);
      console.log("Zoho responde:", response.data);
    }
  }else if(propuestaComercial.idPotential != null){
    let tarifaAux = {
      pesoEstimado: 0,
      volumenEstimado: 0,
      unidadesACobrar: 0,
      tarifaUsd: 0
    }
    data =  createDataPotentialUpdate(null, cabecera.idPotential, tarifaAux, propuestaBase );
    access_token = await getAccessToken();
    response = await updateInZoho("Potentials",data, access_token);
    console.log("Zoho responde:", response.data);
  } 
} 
const SendZohoFromCotizacion = async (cotizacion, propuesta) =>{
  const access_token = await getAccessToken();
  let contacto = await getBuscarContact(access_token, cotizacion.email);
  if(typeof contacto === 'undefined'){
      //crear contacto en CRM
      /*let emailComercial = await getEmailComercial(cotizacion.fk_comercial);
      let id = await zoho.getIdUsersZoho(emailComercial, access_token);
      contacto = await getContacto(cotizacion.fk_cliente, cotizacion.cr_email);
      contacto = zoho.createDataContacto();*/
      contacto = {
          Owner:{
              id:''
          }
      }
      
  }
  const account = await getBuscarCuenta(access_token, cotizacion.fk_cliente);
  if(typeof account === 'undefined'){
      let cliente = await getCliente(cotizacion.fk_cliente);
      const representante = await getRepLegal(cotizacion.fk_cliente);
      cliente = createDataCuenta(cliente, representante,contacto.Owner );
      //console.log("account :", cliente);
      const response = await createInZoho("Accounts", cliente, access_token);
      console.log("Zoho responde", response);
  }   

  const potential = createDataPotential(cotizacion.id, propuesta[0].id, cotizacion.peso, cotizacion.volumen, cotizacion.tarifa, cotizacion.total, cotizacion.idCase, contacto.id, contacto.Owner.id );
  const response1 = await createInZoho("Potentials",potential, access_token);
  await putIdPotential(propuesta[0].id, result2.data[0].details.id);
  console.log("Zoho responde",response1);
  //console.log("potential:", potential);
  const caso = zoho.createDataCase(cotizacion.idCase, "Cerrado");
  const response2 = await updateInZoho("Cases",caso, access_token);
  console.log("Zoho Responde", response2);

} 

/**
 * 
 * @description funciones para sicronizar QIAO - Zoho CRM
 * 
 */

const synchronizeCuenta = async (id) => {

  const access_token = await getAccessToken();
  const clienteAux = await getBuscarCuenta(access_token, id);

  if(typeof clienteAux !== 'undefined'){
    console.log("PUT CLIENTE");
    await SendUpdateCliente(id);
  }else{
    console.log("POST CLIENTE");
    await SendCreateCliente(id);
  }

}

const synchronizeContacto = async (email, fk_comercial) => {

  const access_token = await getAccessToken();
  const contacto = await getBuscarContact(access_token, email);

  if(typeof contacto !== 'undefined'){
    console.log("PUT CONTACTO");
    await SendUpdateRegistroContacto(email, fk_comercial);
  }else{
    console.log("POST CONTACTO");
    await SendCreateRegistroContacto(email, fk_comercial);
  }

}

module.exports = {
  getRefreshToken,
  getAccessToken,
  createInZoho,
  updateInZoho,
  getCliente,
  createDataCuenta,
  createDataCuentaUpdate,
  deleteContact,
  getRegistroContacto,
  getClienteContacto,
  getContactoById,
  createDataContacto,
  createDataContactoUpdate,
  createDataPotential,
  createDataCase,
  getIdUsersZoho,
  getCuentas,
  getBuscarCuenta,
  getBuscarContact,
  getFkComercial,
  getFkComercialByFkCliente,
  getEmailComercial,
  getFkContacto,
  getPropuestaBase,
  calcularTarifa,
  getInformationEtiqueta,
  generateQRCodeImage,
  procesarNombre,
  SendZohoFromCotizacion,
  SendCreateRegistroContacto,
  SendUpdateRegistroContacto,
  SendCreateCliente,
  SendUpdateCliente,
  SendCreateCotizacionFromRegistroContacto,
  SendCreateCotizacionFromPropuestaComercial,
  SendUpdateCotizacionFromTarifas,
  SendUpdateCotizacionFromDeleteTarifa,
  synchronizeCuenta,
  synchronizeContacto
};
