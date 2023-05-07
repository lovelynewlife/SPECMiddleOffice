from typing import List


class Pipeline:
    NAME = "Pipeline"

    def __init__(self):
        self._is_init = False

    def init(self):
        raise NotImplementedError

    def run(self):
        raise NotImplementedError

    def close(self):
        raise NotImplementedError

    def __str__(self):
        return f"Pipeline:{Pipeline.NAME}"


class PipelineChains(Pipeline):
    NAME = "PipelineChains"

    def __init__(self):
        super().__init__()
        self.__chains: List[Pipeline] = []

    def init(self):
        for p in self.__chains:
            p.init()

    def run(self):
        for p in self.__chains:
            p.run()

    def close(self):
        for p in self.__chains:
            p.close()

    def add_step(self, step: Pipeline):
        self.__chains.append(step)

    def add_steps(self, pipes: List[Pipeline]):
        for pipe in pipes:
            self.add_step(pipe)
