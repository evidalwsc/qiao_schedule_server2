const client = require('../server/config/db.client');
const jwt=require('jsonwebtoken');
const lodash= require('lodash');
const moment=require('moment');
const axios=require('axios');

exports.insertNotificationExpDigital=async(obj,callback)=>{
    try{
        const{
            fk_cliente,
            texto,
            fk_notificacion_configuracion,
            visto,
            fk_servicio
        }=obj;

        axios.post(process.env.systemClienteDigital+'token', {
            usuario: process.env.userLoginClienteDigital,
            password: process.env.passwordLoginClienteDigital
          }).then((res1) => {
            if(res1.data && res1.data.token){
                let options={
                    headers:{
                        'Authorization':res1.data.token
                    }
                };

                axios.post(process.env.systemClienteDigital+'notificaciones/cliente/'+fk_cliente, {
                    fk_notificacion_configuracion: fk_notificacion_configuracion,
                    texto:texto,
                    visto:visto,
                    fk_servicio:fk_servicio
                  },options).then((res2) => {
                    if(res2.data){
                        callback(res2);
                    }
                  }).catch((error) => {
                    console.error(error);
                    callback([]);
                  });
            }
          }).catch((error) => {
            console.error(error);
           callback([]);
          });
    }catch(error){
        console.log(error);
	    return false;
    }
};

exports.verifyConfigNotificationExpDigital=async(fk_cliente,fk_notificacion,callback)=>{
    try{
        await axios.post(process.env.systemClienteDigital+'token', {
            usuario: process.env.userLoginClienteDigital,
            password: process.env.passwordLoginClienteDigital
          }).then((res1) => {
            if(res1.data && res1.data.token){
                let options={
                    headers:{
                        'Authorization':res1.data.token
                    }
                };

                axios.post(process.env.systemClienteDigital+'notificaciones/configuracion/'+fk_cliente+'/'+fk_notificacion,null,options).then((res2) => {
                    if(res2.data){
                        callback(res2);
                    }
                  }).catch((error) => {
                    console.error(error);
                    callback([]);
                  });
            }
          }).catch((error) => {
            console.error(error);
           callback([]);
          });
    }catch(error){
        console.log(error);
	    return [];
    }
};



