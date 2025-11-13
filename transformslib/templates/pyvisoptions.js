var options = {
        "interaction": {
            "hover": true,
            "multiselect": false
        },
        "layout": {
            "hierarchical": {
                "enabled": true,
                "direction": "UD",
                "sortMethod": "directed",
                "levelSeparation": 350,
                "nodeSpacing": 300
            }
        },
        "edges": {
            "arrows": {
                "to": {
                    "enabled": true,
                    "scaleFactor": 1
                }
            }
        },
        "physics": {
            "enabled": true,
            "hierarchicalRepulsion": {
                "centralGravity": 0.0,
                "springLength": 200,
                "springConstant": 0.005,
                "nodeDistance": 300,
                "damping": 0.09
            },
            "solver": "hierarchicalRepulsion",
            "stabilization": {
                "enabled": true,
                "iterations": 1000
            }
        }
}
