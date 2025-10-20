// Path: /Data.gs

/**
 * Gets the full JSON data object by parsing the string from the cache or source.
 * @returns {object} The parsed JSON object.
 */
function getFullDataObject() {
    const jsonString = getSuttaJsonString();
    return JSON.parse(jsonString);
}

/**
 * Gets the raw JSON string, using a chunking cache strategy for performance.
 * @returns {string} The raw JSON string.
 */
function getSuttaJsonString() {
    const cache = CacheService.getScriptCache();
    const CHUNK_CACHE_KEY = 'SUTTA_JSON_CHUNK_';
    const CHUNK_COUNT_KEY = 'SUTTA_JSON_CHUNK_COUNT';
    const chunkCount = cache.get(CHUNK_COUNT_KEY);

    if (chunkCount != null) {
        let jsonDataString = '';
        const keys = [];
        for (let i = 0; i < chunkCount; i++) { keys.push(CHUNK_CACHE_KEY + i); }
        const cachedChunks = cache.getAll(keys);
        for (let i = 0; i < chunkCount; i++) {
            if (cachedChunks[keys[i]]) { jsonDataString += cachedChunks[keys[i]]; } 
            else { jsonDataString = ''; break; }
        }
        if (jsonDataString.length > 0) { return jsonDataString; }
    }

    Logger.log('Cache MISS. Fetching data from public GitHub URL.');
    
    // URL không cần token nữa
    const JSON_DATA_URL = "https://raw.githubusercontent.com/vjjda/suttacentral-vj/main/data/public/cips/cips_sutta.json";
    
    // Options không cần header Authorization nữa
    const options = {
      'muteHttpExceptions': true
    };
    
    const response = UrlFetchApp.fetch(JSON_DATA_URL, options);
    
    if (response.getResponseCode() !== 200) {
        throw new Error(`Failed to fetch data. Response code: ${response.getResponseCode()}`);
    }

    const content = response.getContentText();
    const CHUNK_SIZE = 90000;
    const numChunks = Math.ceil(content.length / CHUNK_SIZE);
    const chunks = {};
    for (let i = 0; i < numChunks; i++) {
        chunks[CHUNK_CACHE_KEY + i] = content.substring(i * CHUNK_SIZE, (i + 1) * CHUNK_SIZE);
    }
    chunks[CHUNK_COUNT_KEY] = numChunks.toString();
    cache.putAll(chunks, 21600);
    return content;
}