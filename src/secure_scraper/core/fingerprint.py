"""Helpers for applying deterministic fingerprint overrides."""
from __future__ import annotations

import json
import textwrap
from dataclasses import dataclass
from typing import Optional, Sequence, TYPE_CHECKING

from playwright.async_api import BrowserContext

if TYPE_CHECKING:  # pragma: no cover
    from secure_scraper.config.settings import Settings


@dataclass(frozen=True)
class PluginOverride:
    """Represents a single navigator.plugins entry."""

    name: str
    filename: str
    description: str
    mime_types: Sequence[dict[str, str]]


@dataclass(frozen=True)
class FingerprintOverrides:
    """Structured view of the fields we spoof on navigator/screen/window."""

    user_agent: Optional[str]
    platform: Optional[str]
    language: Optional[str]
    languages: Sequence[str]
    hardware_concurrency: Optional[int]
    max_touch_points: Optional[int]
    vendor: Optional[str]
    product_sub: Optional[str]
    do_not_track: Optional[str]
    screen_width: Optional[int]
    screen_height: Optional[int]
    screen_avail_width: Optional[int]
    screen_avail_height: Optional[int]
    color_depth: Optional[int]
    pixel_depth: Optional[int]
    window_inner_width: Optional[int]
    window_inner_height: Optional[int]
    window_outer_width: Optional[int]
    window_outer_height: Optional[int]
    device_pixel_ratio: Optional[float]
    plugins: Sequence[PluginOverride]
    webgl_vendor: Optional[str]
    webgl_renderer: Optional[str]
    canvas_fingerprint: Optional[str]
    disable_client_hints: bool = False
    device_memory: Optional[float] = None
    oscpu: Optional[str] = None


async def apply_fingerprint_overrides(context: BrowserContext, settings: "Settings") -> None:
    """Attach init scripts that spoof navigator/window/screen properties."""
    overrides = settings.fingerprint_overrides()
    if not overrides:
        return
    script = build_init_script(overrides)
    await context.add_init_script(script)


def build_init_script(overrides: FingerprintOverrides) -> str:
    """Generate the JavaScript payload that applies the overrides."""
    data = {
        "userAgent": overrides.user_agent,
        "platform": overrides.platform,
        "language": overrides.language,
        "languages": list(overrides.languages),
        "hardwareConcurrency": overrides.hardware_concurrency,
        "maxTouchPoints": overrides.max_touch_points,
        "vendor": overrides.vendor,
        "productSub": overrides.product_sub,
        "doNotTrack": overrides.do_not_track,
        "deviceMemory": overrides.device_memory,
        "screen": {
            "width": overrides.screen_width,
            "height": overrides.screen_height,
            "availWidth": overrides.screen_avail_width,
            "availHeight": overrides.screen_avail_height,
            "colorDepth": overrides.color_depth,
            "pixelDepth": overrides.pixel_depth,
        },
        "window": {
            "innerWidth": overrides.window_inner_width,
            "innerHeight": overrides.window_inner_height,
            "outerWidth": overrides.window_outer_width,
            "outerHeight": overrides.window_outer_height,
        },
        "devicePixelRatio": overrides.device_pixel_ratio,
        "plugins": [
            {
                "name": plugin.name,
                "filename": plugin.filename,
                "description": plugin.description,
                "mimeTypes": [dict(mime) for mime in plugin.mime_types],
            }
            for plugin in overrides.plugins
        ],
        "webglVendor": overrides.webgl_vendor,
        "webglRenderer": overrides.webgl_renderer,
        "canvasFingerprint": overrides.canvas_fingerprint,
        "disableClientHints": overrides.disable_client_hints,
        "oscpu": overrides.oscpu,
    }

    payload = json.dumps(data)
    script = f"""
    (() => {{
      const override = {payload};
      const isFirefoxUA = typeof override.userAgent === 'string' && override.userAgent.includes('Firefox');
      const define = (obj, prop, value) => {{
        if (value === undefined || value === null) return;
        try {{
          Object.defineProperty(obj, prop, {{
            get: () => value,
            configurable: true,
          }});
        }} catch (err) {{}}
      }};

      if (override.userAgent) {{
        define(navigator, 'userAgent', override.userAgent);
      }}
      if (override.platform) {{
        define(navigator, 'platform', override.platform);
      }}
      if (override.language) {{
        define(navigator, 'language', override.language);
      }}
      if (Array.isArray(override.languages) && override.languages.length) {{
        const langs = Object.freeze(override.languages.slice());
        define(navigator, 'languages', langs);
      }}
      if (override.hardwareConcurrency !== undefined && override.hardwareConcurrency !== null) {{
        define(navigator, 'hardwareConcurrency', override.hardwareConcurrency);
      }}
      if (override.maxTouchPoints !== undefined && override.maxTouchPoints !== null) {{
        define(navigator, 'maxTouchPoints', override.maxTouchPoints);
      }}
      if (override.vendor !== undefined && override.vendor !== null) {{
        define(navigator, 'vendor', override.vendor);
      }}
      if (override.productSub !== undefined && override.productSub !== null) {{
        define(navigator, 'productSub', override.productSub);
      }}
      if (override.doNotTrack !== undefined && override.doNotTrack !== null) {{
        define(navigator, 'doNotTrack', override.doNotTrack);
        define(window, 'doNotTrack', override.doNotTrack);
      }}
      define(navigator, 'webdriver', false);
      try {{
        Object.defineProperty(navigator, 'deviceMemory', {{
          get: () => (override.deviceMemory === undefined || override.deviceMemory === null ? undefined : override.deviceMemory),
          configurable: true,
        }});
      }} catch (err) {{}}

      const screenProps = override.screen || {{}};
      const setScreenProp = (prop, value) => {{
        if (value === undefined || value === null) return;
        try {{
          Object.defineProperty(window.screen, prop, {{ get: () => value, configurable: true }});
        }} catch (err) {{}}
      }};
      setScreenProp('width', screenProps.width);
      setScreenProp('height', screenProps.height);
      setScreenProp('availWidth', screenProps.availWidth);
      setScreenProp('availHeight', screenProps.availHeight);
      setScreenProp('colorDepth', screenProps.colorDepth);
      setScreenProp('pixelDepth', screenProps.pixelDepth);

      const windowProps = override.window || {{}};
      const setWindowProp = (prop, value) => {{
        if (value === undefined || value === null) return;
        try {{
          Object.defineProperty(window, prop, {{ get: () => value, configurable: true }});
        }} catch (err) {{}}
      }};
      setWindowProp('innerWidth', windowProps.innerWidth);
      setWindowProp('innerHeight', windowProps.innerHeight);
      setWindowProp('outerWidth', windowProps.outerWidth);
      setWindowProp('outerHeight', windowProps.outerHeight);

      if (override.devicePixelRatio !== undefined && override.devicePixelRatio !== null) {{
        setWindowProp('devicePixelRatio', override.devicePixelRatio);
      }}

      const pluginsData = Array.isArray(override.plugins)
        ? override.plugins.map(plugin => ({{
            name: String(plugin.name || ''),
            filename: String(plugin.filename || ''),
            description: String(plugin.description || ''),
            mimeTypes: Array.isArray(plugin.mimeTypes)
              ? plugin.mimeTypes.map(mime => ({{
                  type: String(mime.type || ''),
                  suffixes: String(mime.suffixes || ''),
                  description: String(mime.description || ''),
                }}))
              : [],
          }}))
        : [];

      const pluginEntries = pluginsData.map(data => {{
        const mimeEntries = data.mimeTypes.map(item => ({{
          type: item.type,
          suffixes: item.suffixes,
          description: item.description,
          enabledPlugin: null,
        }}));
        const plugin = {{
          name: data.name,
          filename: data.filename,
          description: data.description,
          length: mimeEntries.length,
          item(index) {{ return mimeEntries[index] || null; }},
          namedItem(name) {{ return mimeEntries.find(entry => entry.type === name) || null; }},
        }};
        mimeEntries.forEach((entry, index) => {{
          entry.enabledPlugin = plugin;
          plugin[index] = entry;
          if (entry.type) {{
            plugin[entry.type] = entry;
          }}
        }});
        return {{ plugin, mimeEntries }};
      }});

      const pluginArray = {{
        length: pluginEntries.length,
        item(index) {{ return pluginEntries[index] ? pluginEntries[index].plugin : null; }},
        namedItem(name) {{
          const found = pluginEntries.find(entry => entry.plugin.name === name);
          return found ? found.plugin : null;
        }},
        refresh() {{}},
        [Symbol.iterator]: function* () {{
          for (const entry of pluginEntries) {{
            yield entry.plugin;
          }}
        }},
      }};
      pluginEntries.forEach((entry, index) => {{
        const plugin = entry.plugin;
        pluginArray[index] = plugin;
        pluginArray[plugin.name] = plugin;
      }});
      define(navigator, 'plugins', pluginArray);

      const mimeEntryList = [];
      pluginEntries.forEach(entry => {{
        entry.mimeEntries.forEach(mime => {{
          mimeEntryList.push(mime);
        }});
      }});

      const mimeTypeArray = {{
        length: mimeEntryList.length,
        item(index) {{ return mimeEntryList[index] || null; }},
        namedItem(name) {{
          return mimeEntryList.find(entry => entry.type === name) || null;
        }},
        [Symbol.iterator]: function* () {{
          for (const entry of mimeEntryList) {{
            yield entry;
          }}
        }},
      }};
      mimeEntryList.forEach((entry, index) => {{
        mimeTypeArray[index] = entry;
        if (entry.type) {{
          mimeTypeArray[entry.type] = entry;
        }}
      }});
      define(navigator, 'mimeTypes', mimeTypeArray);

      if (isFirefoxUA) {{
        try {{
          Object.defineProperty(window, 'InstallTrigger', {{
            get: () => ({{}}),
            configurable: true,
          }});
        }} catch (err) {{}}
        if (override.oscpu) {{
          define(navigator, 'oscpu', override.oscpu);
        }} else if (override.platform) {{
          define(navigator, 'oscpu', override.platform);
        }}
      }} else if (override.oscpu) {{
        define(navigator, 'oscpu', override.oscpu);
      }}

      try {{
        if ('chrome' in window) {{
          delete window.chrome;
        }}
      }} catch (err) {{
        try {{
          Object.defineProperty(window, 'chrome', {{
            get: () => undefined,
            configurable: true,
          }});
        }} catch (err2) {{}}
      }}

      if (override.webglVendor || override.webglRenderer) {{
        const patch = proto => {{
          if (!proto || !proto.getParameter) return;
          const original = proto.getParameter;
          proto.getParameter = function(parameter) {{
            if (parameter === 37445 && override.webglVendor) return override.webglVendor;
            if (parameter === 37446 && override.webglRenderer) return override.webglRenderer;
            return original.call(this, parameter);
          }};
        }};
        if (typeof WebGLRenderingContext !== 'undefined') patch(WebGLRenderingContext.prototype);
        if (typeof WebGL2RenderingContext !== 'undefined') patch(WebGL2RenderingContext.prototype);
      }}

      if (override.canvasFingerprint) {{
        const originalToDataURL = HTMLCanvasElement.prototype.toDataURL;
        HTMLCanvasElement.prototype.toDataURL = function(...args) {{
          if (!args.length || args[0] === 'image/png') {{
            return override.canvasFingerprint;
          }}
          try {{
            return originalToDataURL.apply(this, args);
          }} catch (err) {{
            return override.canvasFingerprint;
          }}
        }};
      }}

      if (override.disableClientHints) {{
        const proto = navigator.__proto__ || Navigator.prototype;
        try {{
          Object.defineProperty(proto, 'userAgentData', {{
            get: () => undefined,
            configurable: true,
          }});
        }} catch (err) {{}}
        try {{
          Object.defineProperty(navigator, 'userAgentData', {{
            get: () => undefined,
            configurable: true,
          }});
        }} catch (err) {{}}
      }}
    }})();
    """
    return textwrap.dedent(script).strip()
