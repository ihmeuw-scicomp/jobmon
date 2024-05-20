const bifrostEnabled = () => {
    const enableEnvVar = process.env.REACT_APP_DEPLOYMENT_TYPE
    try {
        if (enableEnvVar && enableEnvVar.toLowerCase() === "ihme") {
            return true
        }
    } catch {console.log("Bifrost ENV Var not defined. Bifrost will not be loaded")}
    return false
}

export default bifrostEnabled
