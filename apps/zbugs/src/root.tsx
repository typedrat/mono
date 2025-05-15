import {useQuery, ZeroProvider} from '@rocicorp/zero/react';
import {useCallback, useSyncExternalStore} from 'react';
import {useZero} from './hooks/use-zero.ts';
import {zeroRef} from './zero-setup.ts';

export function Root() {
  const z = useSyncExternalStore(
    zeroRef.onChange,
    useCallback(() => zeroRef.value, []),
  );

  if (!z) {
    return null;
  }

  return (
    <ZeroProvider zero={z}>
      <Content />
    </ZeroProvider>
  );
}

function randomMood() {
  const moods = ['sad', 'ok', 'happy'] as const;
  return moods[Math.floor(Math.random() * moods.length)];
}

function randomInt(): number {
  return Math.floor(Math.random() * 100);
}

function TestRow({name, random}: {name: any; random: () => any}) {
  const z = useZero();
  const [labelRow] = useQuery(z.query.label.where('name', '=', 'bug').one());
  return (
    <div>
      <button
        onClick={() => {
          z.mutate.label.change({
            id: labelRow!.id,
            col: name,
            value: random(),
          });
        }}
      >
        Click to Change <code>{name}</code>
      </button>
      <p>
        {name}: {JSON.stringify(labelRow![name])}
      </p>
    </div>
  );
}

function randomText(): string {
  const text = 'Lorem ipsum dolor sit amet, consectetur adipiscing elit.';
  return text.substring(0, Math.floor(Math.random() * text.length));
}

function Content() {
  return (
    <div>
      <TestRow
        name="testEnumArray"
        random={() => [randomMood(), randomMood()]}
      />
      <TestRow name="testMood" random={randomMood} />
      <TestRow name="testIntArray" random={() => [randomInt(), randomInt()]} />
      <TestRow
        name="testTextArray"
        random={() => [randomText(), randomText()]}
      />
    </div>
  );
}
