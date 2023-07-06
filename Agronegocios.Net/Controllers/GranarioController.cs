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
        [Route("FijacionPesificacion/{cuit}/{nroCuenta?}")]
        public HttpResponseMessage FijacionPesificacion(string cuit, string nroCuenta = "")
        {
            try
            {
                ServicioGranario servicio = new ServicioGranario();
                return Request.CreateResponse(HttpStatusCode.OK, servicio.GetFijaciones_Pesificaciones(cuit, nroCuenta));
            }
            catch(Exception ex)
            {
                return Request.CreateResponse(HttpStatusCode.BadRequest,ex.Message);
            }
        }

        [HttpGet]
        [EnableCors(origins: "*", headers: "*", methods: "*")]
        [Route("ReduccionContrato/{cuit}/{nroCuenta?}")]
        public HttpResponseMessage ReduccionContrato(string cuit, string nroCuenta = "")
        {
            try
            {
                ServicioGranario servicio = new ServicioGranario();
                return Request.CreateResponse(HttpStatusCode.OK, servicio.GetReduccionesContratos(cuit, nroCuenta));
            }
            catch (Exception ex)
            {
                return Request.CreateResponse(HttpStatusCode.BadRequest);
            }
        }

        [HttpGet]
        [EnableCors(origins: "*", headers: "*", methods: "*")]
        [Route("saldogranario/{cuit}")]
        public HttpResponseMessage GetDisponibilidadGranaria(string cuit)
        {
            try
            {
                ServicioGranario servicio = new ServicioGranario();
                return Request.CreateResponse(HttpStatusCode.OK, servicio.GetDisponibilidad(cuit));
            }
            catch (Exception ex)
            {
                return Request.CreateResponse(HttpStatusCode.BadRequest, ex.Message);
            }
        }
        [HttpGet]
        [EnableCors(origins: "*", headers: "*", methods: "*")]
        [Route("saldoctacte/{cuit}")]
        public HttpResponseMessage GetSaldoCuentaCte(string cuit)
        {
            try
            {
                ServicioGranario servicio = new ServicioGranario();
                return Request.CreateResponse(HttpStatusCode.OK, servicio.GetSaldoGranario(cuit));
            }
            catch (Exception ex)
            {
                return Request.CreateResponse(HttpStatusCode.BadRequest, ex.Message);
            }
        }
    }
}
