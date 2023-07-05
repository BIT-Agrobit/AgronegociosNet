using Agronegocios.Net.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Web.Http;
using System.Web.Http.Cors;

namespace Agronegocios.Net.Controllers
{
    [RoutePrefix("api/Granario")]
    public class GranarioController : ApiController
    {
        [HttpGet]
        [EnableCors(origins: "*", headers: "*", methods: "*")]
        [Route("FijacionPesificacion/{cuit}")]
        public HttpResponseMessage FijacionPesificacion(string cuit)
        {
            try
            {
                ServicioGranario servicio = new ServicioGranario();
                return Request.CreateResponse(HttpStatusCode.OK, servicio.GetFijaciones_Pesificaciones(cuit));
            }
            catch(Exception ex)
            {
                return Request.CreateResponse(HttpStatusCode.BadRequest,ex.Message);
            }
        }

        [HttpGet]
        [EnableCors(origins: "*", headers: "*", methods: "*")]
        [Route("ReduccionContrato/{cuit}")]
        public HttpResponseMessage ReduccionContrato(string cuit)
        {
            try
            {
                ServicioGranario servicio = new ServicioGranario();
                return Request.CreateResponse(HttpStatusCode.OK, servicio.GetReduccionesContratos(cuit));
            }
            catch
            {
                return Request.CreateResponse(HttpStatusCode.BadRequest);
            }
        }
    }
}
