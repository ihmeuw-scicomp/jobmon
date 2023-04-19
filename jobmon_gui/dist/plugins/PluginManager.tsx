import React from 'react';
import PluginInterface from './PluginInterface'

class PluginManager {
    plugins: PluginInterface[];

    constructor() {
        this.plugins = []
    }

      // Register a plugin
    register(plugin) {
        if (!plugin || !(plugin instanceof PluginInterface)) {
          throw new Error("Invalid plugin");
        }

        // Add the plugin to the list of registered plugins
        this.plugins.push(plugin);
    }

    renderPlugins() {
        // Render all registered plugins
        return this.plugins.map((plugin) => plugin.render());
    }
}

export default PluginManager;