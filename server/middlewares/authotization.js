const jwt=require('jsonwebtoken');
let verifyToken = (req,res,next)=>{
    let token= req.get('Authorization');
    jwt.verify(token, process.env.SECRET, (err,decoded)=>{
        if(err){
            return res.status(401).json({
                success:false,
                err
            })
        }

        req.usuario = decoded.usuario;
        next();
    })
}

module.exports={verifyToken};