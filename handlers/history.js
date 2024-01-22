const client = require('../server/config/db.client');
const jwt=require('jsonwebtoken');
const lodash= require('lodash');
const moment=require('moment');

exports.insertHistoryService=async(obj)=>{
    try{
        const{
            fk_servicio,
            texto,
            fk_usuario,
            estado
        }=obj;

        let exists={
            text:`SELECT id FROM public.servicio_historial WHERE fk_servicio=$1 and texto=$2 and estado=true`,
            values:[fk_servicio,texto]
        };

        let resultExists=await client.query(exists);
        if(resultExists && resultExists.rows){
            let fecha = moment(Date.now()).format("YYYY-MM-DD HH:mm:ss");
            if(resultExists.rows.length>0){
                let sql={
                    text:`UPDATE public.servicio_historial SET
                        texto=$1, fk_servicio=$2, fk_usuario=$3, fecha=$4, estado=$5
                        WHERE id=$6 RETURNING*`,
                    values:[texto,fk_servicio,fk_usuario,fecha,estado,resultExists.rows[0].id]
                };
                let result=await client.query(sql);
                if(result && result.rows){
                    return result.rows;
                }else{
                    return null;
                }
            }else{
                let sql={
                    text:`INSERT INTO public.servicio_historial(
                        texto, fk_servicio, fk_usuario, fecha, estado)
                        VALUES ( $1, $2, $3, $4, $5) RETURNING*`,
                    values:[texto,fk_servicio,fk_usuario,fecha,estado]
                };
        
                let result=await client.query(sql);
                if(result && result.rows){
                    return result.rows;
                }else{
                    return null;
                }
            }
        }
    }catch(error){
        console.log(error);
	    return null;
    }
}