from BCAD_NoiseMit_Tools import WeaverGDBUpdate

if __name__ == "__main__":
    tool = WeaverGDBUpdate()
    params = tool.getParameterInfo()
    tool.execute(params, "")


