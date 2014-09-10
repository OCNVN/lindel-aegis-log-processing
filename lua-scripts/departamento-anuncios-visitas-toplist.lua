local key = 'RECOMENDACION:departamento:' .. KEYS[1] .. ':visitas:conteo'
local keyAnuncioPrefix = 'RECOMENDACION:departamento:' .. KEYS[1] .. ':anuncio:'
local keyToplist = 'RECOMENDACION:departamento:' .. KEYS[1] ..':anuncio:toplist'

local matches = redis.call('ZREVRANGE', key, 0, -1, 'WITHSCORES')
--local anuncios = {}

-- Limpiamos el Set que contiene el toplist antes de llenarlo nuevamente
redis.call('DEL', keyToplist)

for i=1,#matches,2 do
	-- Codigo del anuncio
	local anuncioId = matches[i]
	-- Puntaje
	local visitas = matches[i+1]
	local anuncio = redis.call('HGETALL', keyAnuncioPrefix .. tostring(anuncioId))

	-- Transformamos anuncio en un objeto de lua para codificarlo a JSON posteriormente
	local anuncioObjeto = {}
	for j=1,#anuncio,2 do
		anuncioObjeto[anuncio[j]] = anuncio[j + 1]
	end

	-- Agregamos anuncio al toplist codificando a JSON el anuncio
	redis.call('RPUSH', keyToplist, cjson.encode(anuncioObjeto))

   	--table.insert(anuncios, anuncio) 
end


--return anuncios
return