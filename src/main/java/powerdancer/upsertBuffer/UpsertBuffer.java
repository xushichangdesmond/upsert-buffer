package powerdancer.upsertBuffer;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Function;

public class UpsertBuffer<K, T> {
    final ConcurrentHashMap<K, Node<T>> m = new ConcurrentHashMap<>();
    final ConcurrentLinkedQueue<Node<T>> q = new ConcurrentLinkedQueue<>();
    final Function<T, K> keyMapper;

    public UpsertBuffer(Function<T, K> keyMapper) {
        this.keyMapper = Objects.requireNonNull(keyMapper);
    }

    public Queue<T> asQueue() {
        return new Queue<T>() {
            @Override
            public int size() {
                return q.size();
            }

            @Override
            public boolean isEmpty() {
                return q.isEmpty();
            }

            @Override
            public boolean contains(Object o) {
                Node<T> n = m.get(keyMapper.apply((T) o));
                if (n == null) return false;
                return n.state.val.equals(o);
            }

            @Override
            public Iterator<T> iterator() {
                Iterator<Node<T>> iter = q.iterator();
                return new Iterator<T>() {
                    @Override
                    public boolean hasNext() {
                        return iter.hasNext();
                    }

                    @Override
                    public T next() {
                        return iter.next().state.val;
                    }
                };
            }

            @Override
            public Object[] toArray() {
                return q.toArray();
            }

            @Override
            public <T1> T1[] toArray(T1[] a) {
                return q.toArray(a);
            }

            @Override
            public boolean remove(Object o) {
                throw new UnsupportedOperationException("No plan to support yet");
            }

            @Override
            public boolean containsAll(Collection<?> c) {
                return c.stream().allMatch(this::contains);
            }

            @Override
            public boolean addAll(Collection<? extends T> c) {
                return c.stream().allMatch(this::add);
            }

            @Override
            public boolean removeAll(Collection<?> c) {
                throw new UnsupportedOperationException("No plan to support yet");
            }

            @Override
            public boolean retainAll(Collection<?> c) {
                throw new UnsupportedOperationException("No plan to support yet");
            }

            @Override
            public void clear() {
                while (poll() != null);
            }

            @Override
            public boolean add(T t) {
                return offer(t);
            }

            @Override
            public boolean offer(T t) {
                K k = keyMapper.apply(t);
                Node<T> node = null;
                while (node == null) {
                    node = m.compute(k,
                            (key, n) -> {
                                if (n == null) {
                                    return new Node<>(new Node.State<>(t, 0)); // either managed to generate a new node
                                }
                                Node.State<T> from, to;
                                from = n.state;
                                to = new Node.State<>(t, from.version + 1);
                                if (n.compareAndSetState(from, to))
                                    return n;                                           // or managed to update an existing node
                                return null;                                            // or else try again
                            }
                    );
                }
                if (node.state.version == 0) {
                    q.add(node);
                }
                return true;
            }

            @Override
            public T remove() {
                return Optional.ofNullable(poll()).orElseThrow(()->new NoSuchElementException());
            }

            @Override
            public T poll() {
                Node<T> n = q.poll();
                if (n == null) return null;
                return n.dispose();
            }

            @Override
            public T element() {
                throw new UnsupportedOperationException("will not support because peeking then remove will be an antipattern and may miss updates");
            }

            @Override
            public T peek() {
                throw new UnsupportedOperationException("will not support because peeking then remove will be an antipattern and may miss updates");
            }
        };
    }

    static class Node<T> {
        volatile State<T> state;
        final static VarHandle stateHandle = initStateHandle();

        static VarHandle initStateHandle() {
            try {
                return MethodHandles.lookup().findVarHandle(Node.class, "state", State.class);
            } catch (Exception e) {
                throw new ExceptionInInitializerError(e);
            }
        }

        Node(State<T> state) {
            this.state = state;
        }

        boolean compareAndSetState(State from, State to) {
            return stateHandle.compareAndSet(this, from, to);
        }

        T dispose() {
            return ((State<T>)stateHandle.getAndSet(this, State.deleted)).val;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || (!(o instanceof Node))) return false;
            Node<?> node = (Node<?>) o;
            return Objects.equals(state, node.state);
        }

        @Override
        public int hashCode() {
            return Objects.hash(state);
        }

        static class State<T> {
            static final State deleted = new State(new Object(), -999);

            final T val;
            final int version;

            State(T val, int version) {
                this.val = val;
                this.version = version;
            }

            @Override
            public boolean equals(Object o) {
                if (this == o) return true;
                if (o == null || (!(o instanceof State))) return false;
                State<?> state = (State<?>) o;
                return version == state.version &&
                        Objects.equals(val, state.val);
            }

            @Override
            public int hashCode() {
                return Objects.hash(val, version);
            }
        }
    }
}
