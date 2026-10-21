from mlops.messaging import make_broker
from mlops.settings import PipelineSettings

pipeline_settings = PipelineSettings()
broker = make_broker(pipeline_settings)
