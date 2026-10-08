"""Industry ontology bake-off (PLAN_100).

Kept deliberately minimal: parallel specs add subpackages here
(SPEC_162 ``gold/``, SPEC_163 ``standards/``). Model calling lives in
``providers``, prices in ``pricing``, spend control in ``budget``; nothing in
this package uses the shared people/PE LLM client.
"""
